package store

import (
	"bytes"
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
	_, err := s.client.HeadObject(&s3.HeadObjectInput{
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
func (s *S3) CreateFile(path string, file []byte, meta map[string]string) error {
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket:   s.S3Bucket,
		Key:      aws.String(path),
		Body:     bytes.NewReader(file),
		Metadata: aws.StringMap(meta),
	})

	return err
}

// StreamToFile - записывает содержимое потока в файл
// stream - поток
// path - путь к файлу
func (s *S3) StreamToFile(stream io.Reader, path string) error {
	buf := make([]byte, 1024*1024*5) // 5MB

	resp, err := s.client.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket: s.S3Bucket,
		Key:    aws.String(path),
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

		completedPart, err := s.client.UploadPart(&s3.UploadPartInput{
			Bucket:     s.S3Bucket,
			Key:        aws.String(path),
			UploadId:   resp.UploadId,
			PartNumber: aws.Int64(partNumber),
			Body:       bytes.NewReader(buf[:n]),
		})

		if err != nil {
			if abortErr := s.abortMultipartUpload(resp); abortErr != nil {
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

	_, err = s.completeMultipartUpload(resp, completedParts)

	return err
}

// GetFile - получает файл
// path - путь к файлу
func (s *S3) GetFile(path string) ([]byte, error) {
	stream, err := s.FileReader(path, 0, 0)
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
	stream, err := s.FileReader(path, offset, length)
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
	_range := ""

	if length > 0 {
		_range = fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	} else {
		_range = fmt.Sprintf("bytes=%d-", offset)
	}

	out, err := s.client.GetObject(&s3.GetObjectInput{
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
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: s.S3Bucket,
		Key:    aws.String(path),
	})

	return err
}

// Stat - возвращает информацию о файле
// path - путь к файлу
// os.FileInfo - возвращается неполный
func (s *S3) Stat(path string) (os.FileInfo, map[string]string, error) {
	out, err := s.client.HeadObject(&s3.HeadObjectInput{
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
	list, err := s.client.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: s.S3Bucket,
		Prefix: aws.String(path),
	})

	if err != nil {
		return err
	}

	for _, obj := range list.Contents {
		_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
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
	_, err := s.client.PutObject(&s3.PutObjectInput{
		Bucket: s.S3Bucket,
		Key:    aws.String(path),
		Body:   bytes.NewReader([]byte("")),
	})

	return err
}

// CreateJsonFile - создает json файл
// path - путь к файлу
// data - данные для записи
func (s *S3) CreateJsonFile(path string, data interface{}, meta map[string]string) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return s.CreateFile(path, content, meta)
}

// GetJsonFile - получает файл и десериализует его в переменную
// path - путь к файлу
// file - переменная для записи данных
func (s *S3) GetJsonFile(path string, file interface{}) error {
	content, err := s.GetFile(path)
	if err != nil {
		return err
	}
	if content == nil {
		return nil
	}
	return json.Unmarshal(content, file)
}

func (s *S3) abortMultipartUpload(resp *s3.CreateMultipartUploadOutput) error {
	abortInput := &s3.AbortMultipartUploadInput{
		Bucket:   resp.Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
	}
	_, err := s.client.AbortMultipartUpload(abortInput)
	return err
}

func (s *S3) completeMultipartUpload(resp *s3.CreateMultipartUploadOutput, completedParts []*s3.CompletedPart) (*s3.CompleteMultipartUploadOutput, error) {
	completeInput := &s3.CompleteMultipartUploadInput{
		Bucket:   s.S3Bucket,
		Key:      resp.Key,
		UploadId: resp.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completedParts,
		},
	}
	return s.client.CompleteMultipartUpload(completeInput)
}
