# go-store
os, webdav, s3


##### Интерфейс для работы с файлами
Переменная **STORE_TYPE** определяет с каким хранилищем работает сервис - webdav, s3 либо локальная директория
```go
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
```