package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
)

const (
	LocalStore  = "local"
	WebDavStore = "webdav"
	S3Store     = "s3"
	EmptyStore  = "empty"
	perm        = 0777
	META_PREFIX = ".meta"
)

type StoreConfigIFace interface {
	aws.Config | WebDavConfig | EmptyConfig | LocalConfig
}

type StoreIFace interface {
	IsExist(string) bool
	CreateFile(string, []byte, *time.Time, map[string]string) error
	CopyFile(string, string, *time.Time, map[string]string) error
	MoveFile(string, string) error
	StreamToFile(io.Reader, string, *time.Time) error
	GetFile(path string) ([]byte, error)
	GetFilePartially(string, int64, int64) ([]byte, error)
	FileReader(string, int64, int64) (io.ReadCloser, error)
	RemoveFile(string) error
	CreateJsonFile(string, interface{}, *time.Time, map[string]string) error
	ClearDir(string) error
	GetJsonFile(string, interface{}) error
	Stat(string) (os.FileInfo, map[string]string, error)
	MkdirAll(string) error
}

type Config struct {
	StoreType    string
	EmptyConfig  EmptyConfig
	LocalConfig  LocalConfig
	WebDavConfig WebDavConfig
	S3Config     S3Config
}

type S3Config struct {
	S3Bucket string
	aws.Config
}

type WebDavConfig struct {
	WebDavHost string
	WebDavUser string
	WebDavPass string
}

type EmptyConfig struct{}
type LocalConfig struct{}

func New(cfg Config) (StoreIFace, error) {
	switch cfg.StoreType {
	case LocalStore:
		return NewLocal(cfg.LocalConfig)
	case WebDavStore:
		return NewWebDav(cfg.WebDavConfig)
	case S3Store:
		return NewS3(cfg.S3Config)
	case EmptyStore:
		return NewEmpty(cfg.EmptyConfig)
	default:
		return nil, errors.New("unknown store type")
	}
}

func NewEmpty(cfg EmptyConfig) (StoreIFace, error) {
	s := new(Empty)
	if err := s.init(cfg); err != nil {
		return nil, err
	}
	return s, nil
}

func NewLocal(cfg LocalConfig) (StoreIFace, error) {
	s := new(Local)
	if err := s.init(cfg); err != nil {
		return nil, err
	}
	return s, nil
}

func NewWebDav(cfg WebDavConfig) (StoreIFace, error) {
	s := new(WebDav)
	if err := s.init(cfg); err != nil {
		return nil, err
	}
	return s, nil
}

func NewS3(cfg S3Config) (StoreIFace, error) {
	s := new(S3)
	if err := s.init(cfg); err != nil {
		return nil, err
	}
	return s, nil
}

// Что такое метаданные файла и для чего они нужны?
// Метаданные файла - это информация о файле, которая не является его содержимым.
// Данная информация является дополнительной, на усмотрение разработчика.
// Т.к AWS S3 поддерживает метаданные из коробки, то для остальных хранилищ их приходится хранить в отдельном файле.
// Мета-файл создается вместе с основным файлом и имеет расширение .meta
// Для хранения метаданных используется формат key=value, где key - название метаданных, value - значение метаданных
// При удалении основного файла, удаляется и мета-файл

// meta2Bytes - преобразует метаданные в байты
func meta2Bytes(meta map[string]string) []byte {
	b := new(bytes.Buffer)
	for key, value := range meta {
		fmt.Fprintf(b, "%s=%s\n", key, value)
	}
	return b.Bytes()
}

// bytes2Meta - преобразует байты в метаданные
func bytes2Meta(b []byte) map[string]string {
	meta := make(map[string]string)
	for _, line := range bytes.Split(b, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		pair := bytes.Split(line, []byte{'='})
		if len(pair) != 2 {
			continue
		}
		meta[string(pair[0])] = string(pair[1])
	}
	return meta
}

func overwriteMap(dst, src map[string]string) {
	for k, v := range src {
		dst[k] = v
	}
}
