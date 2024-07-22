package store

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"time"
)

type Local struct {
}

func (l *Local) init(cfg LocalConfig) error {
	return nil
}

// IsExist - проверяет существование файла
// filePath - путь к файлу
func (l *Local) IsExist(filePath string) bool {
	info, err := os.Stat(filePath)
	return err == nil && info.Size() > 0
}

// CreateFile - создает файл
// path - путь к файлу
// file - содержимое файла
// meta - метаданные файла
func (l *Local) CreateFile(path string, file []byte, ttl *time.Time, meta map[string]string) error {
	if meta != nil {
		return os.WriteFile(path+META_PREFIX, meta2Bytes(meta), perm)
	}
	return os.WriteFile(path, file, perm)
}

// CreateFileWithContext - создает файл
// path - путь к файлу
// file - содержимое файла
// meta - метаданные файла
func (l *Local) CreateFileWithContext(ctx context.Context, path string, file []byte, ttl *time.Time, meta map[string]string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return l.CreateFile(path, file, ttl, meta)
	}
}

// CopyFile - копирует файл
// src - исходный путь к файлу
// dst - путь куда копировать
// ttl - время жизни
// meta - метаданные
func (l *Local) CopyFile(src, dst string, ttl *time.Time, meta map[string]string) error {
	//Main file
	source, err := os.Open(src)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destination.Close()

	if _, err := io.Copy(destination, source); err != nil {
		return err
	}

	if err := destination.Sync(); err != nil {
		return err
	}

	//Meta file
	currentMetaInfo, err := os.Stat(src + META_PREFIX)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	if currentMetaInfo != nil && currentMetaInfo.Size() > 0 {
		currentMeta, err := os.ReadFile(src + META_PREFIX)
		if err != nil {
			return err
		}

		currentMetaMap := bytes2Meta(currentMeta)

		for k, v := range meta {
			currentMetaMap[k] = v
		}

		return os.WriteFile(dst+META_PREFIX, meta2Bytes(currentMetaMap), perm)

	} else if meta != nil {
		return os.WriteFile(dst+META_PREFIX, meta2Bytes(meta), perm)
	}

	return nil
}

// CopyFileWithContext - копирует файл
// src - исходный путь к файлу
// dst - путь куда копировать
// ttl - время жизни
// meta - метаданные
func (l *Local) CopyFileWithContext(ctx context.Context, src, dst string, ttl *time.Time, meta map[string]string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return l.CopyFile(src, dst, ttl, meta)
	}
}

// MoveFile - перемещает файл
// src - исходный путь к файлу
// dst - путь куда переместить
func (l *Local) MoveFile(src, dst string) error {
	if _, err := os.Stat(src); os.IsNotExist(err) {
		return ErrFileNotFound
	}

	inputFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer inputFile.Close()

	outputFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer outputFile.Close()

	_, err = io.Copy(outputFile, inputFile)
	if err != nil {
		return err
	}

	inputFile.Close() // for Windows, close before trying to remove: https://stackoverflow.com/a/64943554/246801

	if err := os.Remove(src); err != nil {
		return err
	}

	if err := outputFile.Sync(); err != nil {
		return err
	}

	metaFile, err := os.Stat(src + META_PREFIX)
	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	if metaFile != nil && metaFile.Size() > 0 {
		metaInputFile, err := os.Open(src + META_PREFIX)
		if err != nil {
			return err
		}
		defer metaInputFile.Close()

		metaOutputFile, err := os.Create(dst + META_PREFIX)
		if err != nil {
			return err
		}
		defer metaOutputFile.Close()

		_, err = io.Copy(metaOutputFile, metaInputFile)
		if err != nil {
			return err
		}

		metaInputFile.Close() // for Windows, close before trying to remove: https://stackoverflow.com/a/64943554/246801

		if err := os.Remove(src + META_PREFIX); err != nil {
			return err
		}

		if err := metaOutputFile.Sync(); err != nil {
			return err
		}
	}

	return nil
}

// MoveFileWithContext - перемещает файл
// src - исходный путь к файлу
// dst - путь куда переместить
func (l *Local) MoveFileWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return l.MoveFile(src, dst)
	}
}

// StreamToFile - записывает содержимое потока в файл
// stream - поток
// path - путь к файлу
func (l *Local) StreamToFile(stream io.Reader, path string, ttl *time.Time) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, 1024*1024) // 1MB

	for {
		n, err := stream.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		_, err = file.Write(buf[:n])
		if err != nil {
			return err
		}
	}

	return nil
}

// StreamToFileWithContext - записывает содержимое потока в файл
// stream - поток
// path - путь к файлу
func (l *Local) StreamToFileWithContext(ctx context.Context, stream io.Reader, path string, ttl *time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return l.StreamToFile(stream, path, ttl)
	}
}

// GetFile - возвращает содержимое файла
// path - путь к файлу
func (l *Local) GetFile(path string) ([]byte, error) {
	if !l.IsExist(path) {
		return nil, nil
	}
	return os.ReadFile(path)
}

// GetFileWithContext - возвращает содержимое файла
// path - путь к файлу
func (l *Local) GetFileWithContext(ctx context.Context, path string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return l.GetFile(path)
	}
}

// GetFilePartially - возвращает часть содержимого файла
// path - путь к файлу
// offset - смещение от начала
func (l *Local) GetFilePartially(path string, offset, length int64) ([]byte, error) {
	if !l.IsExist(path) {
		return nil, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	if length < 0 {
		info, _, err := l.Stat(path)
		if err != nil {
			return nil, err
		}
		length = info.Size() - offset
	}

	buf := make([]byte, length)
	_, err = file.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}

	return buf, nil
}

// GetFilePartiallyWithContext - возвращает часть содержимого файла
// path - путь к файлу
// offset - смещение от начала
func (l *Local) GetFilePartiallyWithContext(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return l.GetFilePartially(path, offset, length)
	}
}

// FileReader - открывает файл на чтение
// path - путь к файлу
// offset - смещение от начала
// length - длина
func (l *Local) FileReader(path string, offset, length int64) (io.ReadCloser, error) {
	if !l.IsExist(path) {
		return nil, nil
	}

	return os.Open(path)
}

// FileReaderWithContext - открывает файл на чтение
// path - путь к файлу
// offset - смещение от начала
// length - длина
func (l *Local) FileReaderWithContext(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return l.FileReader(path, offset, length)
	}
}

// RemoveFile - удаляет файл
// path - путь к файлу
func (l *Local) RemoveFile(path string) error {
	os.Remove(path + META_PREFIX)
	err := os.Remove(path)
	if err != nil && os.IsNotExist(err) {
		return ErrFileNotFound
	}
	return err
}

// RemoveFileWithContext - удаляет файл
// path - путь к файлу
func (l *Local) RemoveFileWithContext(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return l.RemoveFile(path)
	}
}

// Stat - возвращает информацию о файле и метаданные
// path - путь к файлу
func (l *Local) Stat(path string) (os.FileInfo, map[string]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, ErrFileNotFound
		}
		return nil, nil, err
	}

	// get meta data
	meta, err := l.GetFile(path + META_PREFIX)
	if err != nil {
		return nil, nil, err
	}

	return info, bytes2Meta(meta), nil
}

// StatWithContext - возвращает информацию о файле и метаданные
// path - путь к файлу
func (l *Local) StatWithContext(ctx context.Context, path string) (os.FileInfo, map[string]string, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
		return l.Stat(path)
	}
}

// ClearDir - очищает директорию
// path - путь к директории
func (l *Local) ClearDir(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return ErrFileNotFound
		}
		return err
	}

	if !info.IsDir() {
		return ErrIsNotDir
	}

	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	names, err := d.Readdirnames(-1)
	if err != nil {
		return err
	}
	for _, name := range names {
		err = os.RemoveAll(filepath.Join(path, name))
		if err != nil {
			return err
		}
	}
	return nil
}

// ClearDirWithContext - очищает директорию
// path - путь к директории
func (l *Local) ClearDirWithContext(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return l.ClearDir(path)
	}
}

// MkdirAll - создает директорию
// path - путь к директории
func (l *Local) MkdirAll(path string) error {
	return os.MkdirAll(path, perm)
}

// MkdirAllWithContext - создает директорию
// path - путь к директории
func (l *Local) MkdirAllWithContext(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return l.MkdirAll(path)
	}
}

// CreateJsonFile - создает файл с данными в формате JSON
// path - путь к файлу
// data - данные
// meta - метаданные
func (l *Local) CreateJsonFile(path string, data interface{}, ttl *time.Time, meta map[string]string) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return l.CreateFile(path, content, ttl, meta)
}

// CreateJsonFileWithContext - создает файл с данными в формате JSON
// path - путь к файлу
// data - данные
// meta - метаданные
func (l *Local) CreateJsonFileWithContext(ctx context.Context, path string, data interface{}, ttl *time.Time, meta map[string]string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return l.CreateJsonFile(path, data, ttl, meta)
	}
}

// GetJsonFile - возвращает содержимое файла в формате JSON
// path - путь к файлу
// file - переменная для десериализации
func (l *Local) GetJsonFile(path string, file interface{}) error {
	content, err := l.GetFile(path)
	if err != nil {
		return err
	}
	if content == nil {
		return nil
	}
	return json.Unmarshal(content, file)
}

// GetJsonFileWithContext - возвращает содержимое файла в формате JSON
// path - путь к файлу
// file - переменная для десериализации
func (l *Local) GetJsonFileWithContext(ctx context.Context, path string, file interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return l.GetJsonFile(path, file)
	}
}
