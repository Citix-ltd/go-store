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
	GetFile(string) ([]byte, error)
	GetFilePartially(string, int64, int64) ([]byte, error)
	FileReader(string, int64, int64) (io.ReadCloser, error)
	RemoveFile(string) error
	CreateJsonFile(string, interface{}, *time.Time, map[string]string) error
	ClearDir(string) error
	GetJsonFile(string, interface{}) error
	Stat(string) (os.FileInfo, map[string]string, error)
	MkdirAll(string) error
	// with ctx
	CreateFileWithContext(context.Context, string, []byte, *time.Time, map[string]string) error
	CopyFileWithContext(context.Context, string, string, *time.Time, map[string]string) error
	MoveFileWithContext(context.Context, string, string) error
	StreamToFileWithContext(context.Context, io.Reader, string, *time.Time) error
	GetFileWithContext(context.Context, string) ([]byte, error)
	GetFilePartiallyWithContext(context.Context, string, int64, int64) ([]byte, error)
	FileReaderWithContext(context.Context, string, int64, int64) (io.ReadCloser, error)
	RemoveFileWithContext(context.Context, string) error
	CreateJsonFileWithContext(context.Context, string, interface{}, *time.Time, map[string]string) error
	ClearDirWithContext(context.Context, string) error
	GetJsonFileWithContext(context.Context, string, interface{}) error
	StatWithContext(context.Context, string) (os.FileInfo, map[string]string, error)
	MkdirAllWithContext(context.Context, string) error
}
```