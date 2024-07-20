package store

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"time"

	"github.com/studio-b12/gowebdav"
)

type WebDav struct {
	client *gowebdav.Client
}

func (w *WebDav) init(cfg WebDavConfig) error {
	w.client = gowebdav.NewClient(cfg.WebDavHost, cfg.WebDavUser, cfg.WebDavPass)
	return nil
}

// IsExist - проверяет существование файла
// filePath - путь к файлу
func (w *WebDav) IsExist(filePath string) bool {
	info, err := w.client.Stat(filePath)
	return err == nil && info.Size() > 0
}

// CreateFile - создает файл
// path - путь к файлу
// file - содержимое файла
// meta - метаданные файла
func (w *WebDav) CreateFile(path string, file []byte, ttl *time.Time, meta map[string]string) error {
	if meta != nil {
		if err := w.client.Write(path+META_PREFIX, meta2Bytes(meta), perm); err != nil {
			return err
		}
	}

	return w.client.Write(path, file, perm)
}

// CreateFileWithContext - создает файл
// path - путь к файлу
// file - содержимое файла
// meta - метаданные файла
func (w *WebDav) CreateFileWithContext(ctx context.Context, path string, file []byte, ttl *time.Time, meta map[string]string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return w.CreateFile(path, file, ttl, meta)
	}
}

// CopyFile - копирует файл
// src - исходный путь к файлу
// dst - путь куда копировать
// ttl - время жизни
// meta - метаданные
func (w *WebDav) CopyFile(src, dst string, ttl *time.Time, meta map[string]string) error {
	currMetaIsExist := w.IsExist(src + META_PREFIX)

	if currMetaIsExist {
		currentMeta, err := w.GetFile(src + META_PREFIX)
		if err != nil {
			return err
		}

		currentMetaMap := bytes2Meta(currentMeta)

		for k, v := range meta {
			currentMetaMap[k] = v
		}

		if err := w.client.Write(dst+META_PREFIX, meta2Bytes(currentMetaMap), perm); err != nil {
			return err
		}
	}

	return w.client.Copy(src, dst, true)
}

// CopyFileWithContext - копирует файл
// src - исходный путь к файлу
// dst - путь куда копировать
// ttl - время жизни
// meta - метаданные
func (w *WebDav) CopyFileWithContext(ctx context.Context, src, dst string, ttl *time.Time, meta map[string]string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return w.CopyFile(src, dst, ttl, meta)
	}
}

// MoveFile - перемещает файл
// src - исходный путь к файлу
// dst - путь куда переместить
func (w *WebDav) MoveFile(src, dst string) error {
	w.client.Rename(src+META_PREFIX, dst+META_PREFIX, true)
	return w.client.Rename(src, dst, true)
}

// MoveFileWithContext - перемещает файл
// src - исходный путь к файлу
// dst - путь куда переместить
func (w *WebDav) MoveFileWithContext(ctx context.Context, src, dst string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return w.MoveFile(src, dst)
	}
}

// StreamToFile - записывает содержимое потока в файл
// stream - поток
// path - путь к файлу
func (w *WebDav) StreamToFile(stream io.Reader, path string, ttl *time.Time) error {
	return w.client.WriteStream(path, stream, perm)
}

// StreamToFileWithContext - записывает содержимое потока в файл
// stream - поток
// path - путь к файлу
func (w *WebDav) StreamToFileWithContext(ctx context.Context, stream io.Reader, path string, ttl *time.Time) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return w.StreamToFile(stream, path, ttl)
	}

}

// GetFile - возвращает содержимое файла
// path - путь к файлу
func (w *WebDav) GetFile(path string) ([]byte, error) {
	if !w.IsExist(path) {
		return nil, nil
	}
	return w.client.Read(path)
}

// GetFileWithContext - возвращает содержимое файла
// path - путь к файлу
func (w *WebDav) GetFileWithContext(ctx context.Context, path string) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return w.GetFile(path)
	}
}

// GetFilePartially - возвращает часть содержимого файла
// path - путь к файлу
// offset - смещение
// length - длина
func (w *WebDav) GetFilePartially(path string, offset, length int64) ([]byte, error) {
	if !w.IsExist(path) {
		return nil, nil
	}

	stream, err := w.client.ReadStreamRange(path, offset, length)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(stream)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// GetFilePartiallyWithContext - возвращает часть содержимого файла
// path - путь к файлу
// offset - смещение
// length - длина
func (w *WebDav) GetFilePartiallyWithContext(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return w.GetFilePartially(path, offset, length)
	}
}

// FileReader - возвращает io.ReadCloser для чтения файла
// path - путь к файлу
// offset - смещение
// length - длина
func (w *WebDav) FileReader(path string, offset, length int64) (io.ReadCloser, error) {
	return w.client.ReadStreamRange(path, offset, length)
}

// FileReaderWithContext - возвращает io.ReadCloser для чтения файла
// path - путь к файлу
// offset - смещение
// length - длина
func (w *WebDav) FileReaderWithContext(ctx context.Context, path string, offset, length int64) (io.ReadCloser, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return w.FileReader(path, offset, length)
	}
}

// RemoveFile - удаляет файл
// path - путь к файлу
func (w *WebDav) RemoveFile(path string) error {
	w.client.Remove(path + META_PREFIX)
	return w.client.Remove(path)
}

// RemoveFileWithContext - удаляет файл
// path - путь к файлу
func (w *WebDav) RemoveFileWithContext(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return w.RemoveFile(path)
	}
}

// Stat - возвращает информацию о файле и метаданные
// path - путь к файлу
func (w *WebDav) Stat(path string) (os.FileInfo, map[string]string, error) {
	info, err := w.client.Stat(path)
	if err != nil {
		return nil, nil, err
	}

	isExist := w.IsExist(path + META_PREFIX)
	if !isExist {
		return info, nil, nil
	}

	meta, err := w.client.Read(path + META_PREFIX)
	if err != nil {
		return nil, nil, err
	}

	return info, bytes2Meta(meta), nil
}

// StatWithContext - возвращает информацию о файле и метаданные
// path - путь к файлу
func (w *WebDav) StatWithContext(ctx context.Context, path string) (os.FileInfo, map[string]string, error) {
	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	default:
		return w.Stat(path)
	}
}

// ClearDir - очищает директорию
// path - путь к директории
func (w *WebDav) ClearDir(path string) error {
	files, _ := w.client.ReadDir(path)
	for _, file := range files {
		if err := w.client.Remove(path + "/" + file.Name()); err != nil {
			return err
		}
	}
	return nil
}

// ClearDirWithContext - очищает директорию
// path - путь к директории
func (w *WebDav) ClearDirWithContext(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return w.ClearDir(path)
	}
}

// MkdirAll - создает директорию
// path - путь к директории
func (w *WebDav) MkdirAll(path string) error {
	return w.client.MkdirAll(path, perm)
}

// MkdirAllWithContext - создает директорию
// path - путь к директории
func (w *WebDav) MkdirAllWithContext(ctx context.Context, path string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return w.MkdirAll(path)
	}
}

// CreateJsonFile - создает файл с данными в формате JSON
// path - путь к файлу
// data - данные
// meta - метаданные
func (w *WebDav) CreateJsonFile(path string, data interface{}, ttl *time.Time, meta map[string]string) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return w.CreateFile(path, content, ttl, meta)
}

// CreateJsonFileWithContext - создает файл с данными в формате JSON
// path - путь к файлу
// data - данные
// meta - метаданные
func (w *WebDav) CreateJsonFileWithContext(ctx context.Context, path string, data interface{}, ttl *time.Time, meta map[string]string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return w.CreateJsonFile(path, data, ttl, meta)
	}
}

// GetJsonFile - возвращает данные из файла в формате JSON
// path - путь к файлу
// file - переменная для записи данных
func (w *WebDav) GetJsonFile(path string, file interface{}) error {
	content, err := w.GetFile(path)
	if err != nil {
		return err
	}
	if content == nil {
		return nil
	}
	return json.Unmarshal(content, file)
}

// GetJsonFileWithContext - возвращает данные из файла в формате JSON
// path - путь к файлу
// file - переменная для записи данных
func (w *WebDav) GetJsonFileWithContext(ctx context.Context, path string, file interface{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return w.GetJsonFile(path, file)
	}
}
