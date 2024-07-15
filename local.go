package store

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
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
func (l *Local) CreateFile(path string, file []byte, meta map[string]string) error {
	if meta != nil {
		if err := os.WriteFile(path+META_PREFIX, meta2Bytes(meta), perm); err != nil {
			return err
		}
	}
	return os.WriteFile(path, file, perm)
}

// StreamToFile - записывает содержимое потока в файл
// stream - поток
// path - путь к файлу
func (l *Local) StreamToFile(stream io.Reader, path string) error {
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

// GetFile - возвращает содержимое файла
// path - путь к файлу
func (l *Local) GetFile(path string) ([]byte, error) {
	if !l.IsExist(path) {
		return nil, nil
	}
	return os.ReadFile(path)
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

// RemoveFile - удаляет файл
// path - путь к файлу
func (l *Local) RemoveFile(path string) error {
	os.Remove(path + META_PREFIX)
	return os.Remove(path)
}

// Stat - возвращает информацию о файле и метаданные
// path - путь к файлу
func (l *Local) Stat(path string) (os.FileInfo, map[string]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}

	isExist := l.IsExist(path + META_PREFIX)
	if !isExist {
		return info, nil, nil
	}

	meta, err := os.ReadFile(path + META_PREFIX)
	if err != nil {
		return nil, nil, err
	}

	return info, bytes2Meta(meta), nil
}

// ClearDir - очищает директорию
// path - путь к директории
func (l *Local) ClearDir(path string) error {
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

// MkdirAll - создает директорию
// path - путь к директории
func (l *Local) MkdirAll(path string) error {
	return os.MkdirAll(path, perm)
}

// CreateJsonFile - создает файл с данными в формате JSON
// path - путь к файлу
// data - данные
// meta - метаданные
func (l *Local) CreateJsonFile(path string, data interface{}, meta map[string]string) error {
	content, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	return l.CreateFile(path, content, meta)
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
