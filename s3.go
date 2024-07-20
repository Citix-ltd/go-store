package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// File is our structure for a given file
type File struct {
	name     string
	size     int64
	modified time.Time
	isdir    bool
}

func (f File) Name() string {
	return f.name
}

func (f File) Size() int64 {
	return f.size
}

func (f File) Mode() os.FileMode {
	// TODO check webdav perms
	if f.isdir {
		return 0775 | os.ModeDir
	}

	return 0664
}

func (f File) ModTime() time.Time {
	return f.modified
}

func (f File) IsDir() bool {
	return f.isdir
}

func (f File) Sys() interface{} {
	return nil
}

type S3 struct {
	client   *s3.S3
	S3Bucket *string
}

func (s *S3) init(cfg S3Config) error {
	s.client = s3.New(session.Must(session.NewSession(&cfg.Config)))
	s.S3Bucket = aws.String(cfg.S3Bucket)
	return nil
}

// IsExist - проверяет существование файла
// filePath - путь к файлу
func (s *S3) IsExist(filePath string) bool {
	_, err := s.client.HeadObject(
		&s3.HeadObjectInput{
			Bucket: s.S3Bucket,
			Key:    aws.String(filePath),
		})

	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "NotFound" {
				return false
			}
		}
		return false
	}

	return true
}

// CreateFile - создает файл
// path - путь к файлу
// file - содержимое файла
// meta - метаданные файла
func (s *S3) CreateFile(path string, file []byte, ttl *time.Time, meta map[string]string) error {
	return s.CreateFileWithContext(context.Background(), path, file, ttl, meta)
}

// CreateFileWithContext - создает файл
// path - путь к файлу
// file - содержимое файла
// meta - метаданные файла
func (s *S3) CreateFileWithContext(ctx context.Context, path string, file []byte, ttl *time.Time, meta map[string]string) error {
	_, err := s.client.PutObjectWithContext(
		ctx,
		&s3.PutObjectInput{
			Bucket:   s.S3Bucket,
			Key:      aws.String(path),
			Body:     bytes.NewReader(file),
			Metadata: aws.StringMap(meta),
			Expires:  ttl,
		})

	return err
}

// CopyFile - копирует файл
// src - исходный путь к файлу
// dst - путь куда копировать
// ttl - время жизни
// meta - метаданные
func (s *S3) CopyFile(src, dst string, ttl *time.Time, meta map[string]string) error {
	return s.CopyFileWithContext(context.Background(), src, dst, ttl, meta)
}

// CopyFileWithContext - копирует файл
// src - исходный путь к файлу
// dst - путь куда копировать
// ttl - время жизни
// meta - метаданные
func (s *S3) CopyFileWithContext(ctx context.Context, src, dst string, ttl *time.Time, meta map[string]string) error {
	// Тянем метаданные из исходного файла
	// и обогащаем их новыми данными если таковые есть
	head, err := s.client.HeadObjectWithContext(
		ctx,
		&s3.HeadObjectInput{
			Bucket: s.S3Bucket,
			Key:    aws.String(src),
		})

	if err != nil {
		return err
	}

	currentMeta := aws.StringValueMap(head.Metadata)

	for k, v := range meta {
		currentMeta[k] = v
	}

	_, err = s.client.CopyObjectWithContext(
		ctx,
		&s3.CopyObjectInput{
			Bucket:            s.S3Bucket,
			CopySource:        aws.String(fmt.Sprintf("%s/%s", *s.S3Bucket, src)),
			Key:               aws.String(dst),
			Metadata:          aws.StringMap(currentMeta),
			MetadataDirective: aws.String("REPLACE"),
			Expires:           ttl,
		})

	return err
}

// MoveFile - перемещает файл
// src - исходный путь к файлу
// dst - путь куда переместить
func (s *S3) MoveFile(src, dst string) error {
	return s.MoveFileWithContext(context.Background(), src, dst)
}

// MoveFileWithContext - перемещает файл
// src - исходный путь к файлу
// dst - путь куда переместить
func (s *S3) MoveFileWithContext(ctx context.Context, src, dst string) error {
	_, err := s.client.CopyObjectWithContext(
		ctx,
		&s3.CopyObjectInput{
			Bucket:     s.S3Bucket,
			CopySource: aws.String(fmt.Sprintf("%s/%s", *s.S3Bucket, src)),
			Key:        aws.String(dst),
		})

	if err != nil {
		return err
	}

	err = s.client.WaitUntilObjectExistsWithContext(
		ctx,
		&s3.HeadObjectInput{
			Bucket: s.S3Bucket,
			Key:    aws.String(dst),
		})

	if err != nil {
		return err
	}

	_, err = s.client.DeleteObjectWithContext(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: s.S3Bucket,
			Key:    aws.String(src),
		})

	if err != nil {
		return err
	}

	err = s.client.WaitUntilObjectNotExistsWithContext(
		ctx,
		&s3.HeadObjectInput{
			Bucket: s.S3Bucket,
			Key:    aws.String(src),
		})

	if err != nil {
		return err
	}

	return nil
}

// StreamToFile - записывает содержимое потока в файл
// stream - поток
// path - путь к файлу
func (s *S3) StreamToFile(stream io.Reader, path string, ttl *time.Time) error {
	return s.StreamToFileWithContext(context.Background(), stream, path, ttl)
}

// StreamToFile - записывает содержимое потока в файл
// stream - поток
// path - путь к файлу
func (s *S3) StreamToFileWithContext(ctx context.Context, stream io.Reader, path string, ttl *time.Time) error {
	buf := make([]byte, 1024*1024*5) // 5MB

	resp, err := s.client.CreateMultipartUploadWithContext(
		ctx,
		&s3.CreateMultipartUploadInput{
			Bucket:  s.S3Bucket,
			Key:     aws.String(path),
			Expires: ttl,
		})
	if err != nil {
		return err
	}

	var partNumber int64 = 1
	var completedParts []*s3.CompletedPart

	for {
		n, err := stream.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		//fmt.Println("Uploading part", partNumber, "of", path, "size:", n)

		completedPart, err := s.client.UploadPartWithContext(
			ctx,
			&s3.UploadPartInput{
				Bucket:     s.S3Bucket,
				Key:        aws.String(path),
				UploadId:   resp.UploadId,
				PartNumber: aws.Int64(partNumber),
				Body:       bytes.NewReader(buf[:n]),
			})

		if err != nil {
			if abortErr := s.abortMultipartUpload(ctx, resp); abortErr != nil {
				return abortErr
			}
			return err
		}

		completedParts = append(completedParts, &s3.CompletedPart{
			ETag:       completedPart.ETag,
			PartNumber: aws.Int64(partNumber),
		})

		partNumber++
	}

	_, err = s.completeMultipartUpload(ctx, resp, completedParts)

	return err
}

// GetFile - получает файл
// path - путь к файлу
func (s *S3) GetFile(path string) ([]byte, error) {
	return s.GetFileWithContext(context.Background(), path)
}

// GetFileWithContext - получает файл
// path - путь к файлу
func (s *S3) GetFileWithContext(ctx context.Context, path string) ([]byte, error) {
	stream, err := s.FileReaderWithContext(ctx, path, 0, 0)
	if err != nil {
		return nil, err
	}

	defer stream.Close()

	return io.ReadAll(stream)
}

// GetFilePartially - получает часть файла
// path - путь к файлу
// offset - смещение от начала
// length - длина
// https://www.rfc-editor.org/rfc/rfc9110.html#name-range
func (s *S3) GetFilePartially(path string, offset, length int64) ([]byte, error) {
	return s.GetFilePartiallyWithContext(context.Background(), path, offset, length)
}

// GetFilePartiallyWithContext - получает часть файла
// path - путь к файлу
// offset - смещение от начала
// length - длина
// https://www.rfc-editor.org/rfc/rfc9110.html#name-range
func (s *S3) GetFilePartiallyWithContext(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	stream, err := s.FileReaderWithContext(ctx, path, offset, length)
	if err != nil {
		return nil, err
	}

	defer stream.Close()

	return io.ReadAll(stream)
}

// FileReader - возвращает io.ReadCloser для чтения файла
// path - путь к файлу
// offset - смещение от начала
// length - длина
func (s *S3) FileReader(path string, offset, length int64) (io.ReadCloser, error) {
	return s.FileReaderWithContext(context.Background(), path, offset, length)
}

// FileReaderWithContext - возвращает io.ReadCloser для чтения файла
// path - путь к файлу
// offset - смещение от начала
// length - длина
func (s *S3) FileReaderWithContext(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	_range := ""

	if length > 0 {
		_range = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	} else {
		_range = fmt.Sprintf("bytes=%d-", offset)
	}

	out, err := s.client.GetObjectWithContext(
		ctx,
		&s3.GetObjectInput{
			Bucket: s.S3Bucket,
			Key:    aws.String(path),
			Range:  aws.String(_range),
		})

	if err != nil {
		return nil, err
	}

	return out.Body, nil
}

// RemoveFile - удаляет файл
// path - путь к файлу
func (s *S3) RemoveFile(path string) error {
	return s.RemoveFileWithContext(context.Background(), path)
}

// RemoveFileWithContext - удаляет файл
// path - путь к файлу
func (s *S3) RemoveFileWithContext(ctx context.Context, path string) error {
	_, err := s.client.DeleteObjectWithContext(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: s.S3Bucket,
			Key:    aws.String(path),
		})

	return err
}

// Stat - возвращает информацию о файле
// path - путь к файлу
// os.FileInfo - возвращается неполный
func (s *S3) Stat(path string) (os.FileInfo, map[string]string, error) {
	return s.StatWithContext(context.Background(), path)
}

// Stat - возвращает информацию о файле
// path - путь к файлу
// os.FileInfo - возвращается неполный
func (s *S3) StatWithContext(ctx context.Context, path string) (os.FileInfo, map[string]string, error) {
	out, err := s.client.HeadObjectWithContext(
		ctx,
		&s3.HeadObjectInput{
			Bucket: s.S3Bucket,
			Key:    aws.String(path),
		})

	if err != nil {
		return nil, nil, err
	}

	f := new(File)
	f.name = path
	f.size = *out.ContentLength
	f.modified = *out.LastModified

	return f, aws.StringValueMap(out.Metadata), nil
}

// ClearDir - очищает директорию
// path - путь к директории
func (s *S3) ClearDir(path string) error {
	return s.ClearDirWithContext(context.Background(), path)
}

// ClearDir - очищает директорию
// path - путь к директории
func (s *S3) ClearDirWithContext(ctx context.Context, path string) error {
	list, err := s.client.ListObjectsV2WithContext(
		ctx,
		&s3.ListObjectsV2Input{
			Bucket: s.S3Bucket,
			Prefix: aws.String(path),
		})

	if err != nil {
		return err
	}

	for _, obj := range list.Contents {
		_, err := s.client.DeleteObjectWithContext(
			ctx,
			&s3.DeleteObjectInput{
				Bucket: s.S3Bucket,
				Key:    obj.Key,
			})
		if err != nil {
			return err
		}
	}

	return nil
}

// MkdirAll - создает директорию
// path - путь к директории
func (s *S3) MkdirAll(path string) error {
	return s.MkdirAllWithContext(context.Background(), path)
}

// MkdirAllWithContext - создает директорию
// path - путь к директории
func (s *S3) MkdirAllWithContext(ctx context.Context, path string) error {
	_, err := s.client.PutObjectWithContext(
		ctx,
		&s3.PutObjectInput{
			Bucket: s.S3Bucket,
			Key:    aws.String(path),
			Body:   bytes.NewReader([]byte("")),
		})

	return err
}

// CreateJsonFile - создает json файл
// path - путь к файлу
// data - данные для записи
func (s *S3) CreateJsonFile(path string, data interface{}, ttl *time.Time, meta map[string]string) error {
	return s.CreateJsonFileWithContext(context.Background(), path, data, ttl, meta)
}

// CreateJsonFileWithContext - создает json файл
// path - путь к файлу
// data - данные для записи
func (s *S3) CreateJsonFileWithContext(ctx context.Context, path string, data interface{}, ttl *time.Time, meta map[string]string) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return s.CreateFile(path, content, ttl, meta)
}

// GetJsonFile - получает файл и десериализует его в переменную
// path - путь к файлу
// file - переменная для записи данных
func (s *S3) GetJsonFile(path string, file interface{}) error {
	return s.GetJsonFileWithContext(context.Background(), path, file)
}

// GetJsonFileWithContext - получает файл и десериализует его в переменную
// path - путь к файлу
// file - переменная для записи данных
func (s *S3) GetJsonFileWithContext(ctx context.Context, path string, file interface{}) error {
	content, err := s.GetFileWithContext(ctx, path)
	if err != nil {
		return err
	}
	if content == nil {
		return nil
	}
	return json.Unmarshal(content, file)
}

func (s *S3) abortMultipartUpload(ctx context.Context, resp *s3.CreateMultipartUploadOutput) error {
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := s.client.AbortMultipartUploadWithContext(ctx, abortInput)
	return err
}

func (s *S3) completeMultipartUpload(ctx context.Context, resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   s.S3Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return s.client.CompleteMultipartUploadWithContext(ctx, completeInput)
}
