# go-store
os, webdav, s3


##### Интерфейс для работы с файлами
Переменная **STORE_TYPE** определяет с каким хранилищем работает сервис - webdav, s3 либо локальная директория
```go
type StoreIFace interface {
	IsExist(string) bool
	CreateFile(string, []byte, map[string]*string) error
	StreamToFile(stream io.Reader, path string) error
	GetFile(path string) ([]byte, error)
	GetFilePartially(path string, offset, length int64) ([]byte, error)
	FileReader(path string, offset, length int64) (io.ReadCloser, error)
	RemoveFile(path string) error
	CreateJsonFile(string, interface{}, map[string]*string) error
	ClearDir(string) error
	GetJsonFile(string, interface{}) error
	Stat(string) (os.FileInfo, map[string]*string, error)
	MkdirAll(string) error
}
```