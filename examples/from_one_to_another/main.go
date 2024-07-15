package main

import (
	"flag"
	"fmt"
	"time"

	"github.com/Citix-ltd/go-store"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

const (
	_S3Bucket      = "/my-bucket"
	_S3Region      = "eu-central-1"
	_S3AccessId    = "hdCTitOcGEr9rNw65Uo2"
	_S3AccessKey   = "gHl2y24YYHJQi1rrsjJmgTL2psN88JGeJRkL6ShZ"
	_S3AccessToken = ""
	_S3Endpoint    = "http://localhost:9000"

	_WebDavHost = "http://localhost:8080"
	_WebDavUser = "user"
	_WebDavPass = "password"

	_500MBFileName = "test-500M.txt"
	_1BFileName    = "example.txt"

	_500MBFile = "../files/" + _500MBFileName
	_1BFile    = "../files/" + _1BFileName
)

var choice int

func main() {
	flag.IntVar(&choice, "choice", 1, "1 - from s3 to local, 2 - from local to s3, 3 - from webdav to local, 4 - from local to webdav, 5 - from s3 to webdav, 6 - from webdav to s3, 7 - from s3 to webdav partially")
	flag.Parse()

	switch choice {
	case 1:
		fromS3ToLocal()
	case 2:
		fromLocalToS3()
	case 3:
		fromWebDavToLocal()
	case 4:
		fromLocalToWebDav()
	case 5:
		fromS3ToWebDav()
	case 6:
		fromWebDavToS3()
	case 7:
		fromS3ToWebDavPartially()
	default:
		fmt.Println("Invalid choice")
	}
}

func fromS3ToLocal() {
	s3Store, _ := store.NewS3(store.S3Config{
		S3Bucket: _S3Bucket,
		Config: aws.Config{
			Region:      aws.String(_S3Region),
			Credentials: credentials.NewStaticCredentials(_S3AccessId, _S3AccessKey, _S3AccessToken),
			Endpoint:    aws.String(_S3Endpoint),
		},
	})

	localStore, _ := store.New(store.Config{
		StoreType:   store.LocalStore,
		LocalConfig: store.LocalConfig{},
	})

	stream, err := s3Store.FileReader("example.txt", 0, 0)
	if err != nil {
		panic(err)
	}

	err = localStore.StreamToFile(stream, "store/example.txt")
	if err != nil {
		panic(err)
	}
}

func fromLocalToS3() {
	localStore, _ := store.NewLocal(store.LocalConfig{})

	s3Store, _ := store.NewS3(store.S3Config{
		S3Bucket: _S3Bucket,
		Config: aws.Config{
			Region:      aws.String(_S3Region),
			Credentials: credentials.NewStaticCredentials(_S3AccessId, _S3AccessKey, _S3AccessToken),
			Endpoint:    aws.String(_S3Endpoint),
		},
	})

	stream, err := localStore.FileReader(_500MBFile, 0, 0)
	if err != nil {
		panic(err)
	}
	defer stream.Close()

	err = s3Store.StreamToFile(stream, _500MBFile)
	if err != nil {
		panic(err)
	}
}

func fromWebDavToLocal() {
	webDavStore, _ := store.NewWebDav(store.WebDavConfig{
		WebDavHost: _WebDavHost,
		WebDavUser: _WebDavUser,
		WebDavPass: _WebDavPass,
	})

	localStore, _ := store.NewLocal(store.LocalConfig{})

	stream, err := webDavStore.FileReader(_1BFile, 0, 0)
	if err != nil {
		panic(err)
	}
	defer stream.Close()

	err = localStore.StreamToFile(stream, _1BFile)
	if err != nil {
		panic(err)
	}

	fmt.Println("fromWebDavToLocal Done")
}

func fromLocalToWebDav() {
	localStore, _ := store.NewLocal(store.LocalConfig{})

	webDavStore, _ := store.NewWebDav(store.WebDavConfig{
		WebDavHost: _WebDavHost,
		WebDavUser: _WebDavUser,
		WebDavPass: _WebDavPass,
	})

	stream, err := localStore.FileReader(_1BFile, 0, 0)
	if err != nil {
		panic(err)
	}
	defer stream.Close()

	err = webDavStore.StreamToFile(stream, _1BFile)
	if err != nil {
		panic(err)
	}

	fmt.Println("fromLocalToWebDav Done")
}

func fromS3ToWebDav() {
	s3Store, _ := store.NewS3(store.S3Config{
		S3Bucket: _S3Bucket,
		Config: aws.Config{
			Region:      aws.String(_S3Region),
			Credentials: credentials.NewStaticCredentials(_S3AccessId, _S3AccessKey, _S3AccessToken),
			Endpoint:    aws.String(_S3Endpoint),
		},
	})

	webDavStore, _ := store.NewWebDav(store.WebDavConfig{
		WebDavHost: _WebDavHost,
		WebDavUser: _WebDavUser,
		WebDavPass: _WebDavPass,
	})

	stream, err := s3Store.FileReader(_500MBFile, 0, 0)
	if err != nil {
		panic(err)
	}
	defer stream.Close()

	err = webDavStore.StreamToFile(stream, _500MBFile)
	if err != nil {
		panic(err)
	}

	fmt.Println("fromS3ToWebDav Done")
}

func fromWebDavToS3() {
	webDavStore, _ := store.NewWebDav(store.WebDavConfig{
		WebDavHost: _WebDavHost,
		WebDavUser: _WebDavUser,
		WebDavPass: _WebDavPass,
	})

	s3Store, _ := store.NewS3(store.S3Config{
		S3Bucket: _S3Bucket,
		Config: aws.Config{
			Region:      aws.String(_S3Region),
			Credentials: credentials.NewStaticCredentials(_S3AccessId, _S3AccessKey, _S3AccessToken),
			Endpoint:    aws.String(_S3Endpoint),
		},
	})

	stream, err := webDavStore.FileReader(_1BFile, 0, 0)
	if err != nil {
		panic(err)
	}
	defer stream.Close()

	err = s3Store.StreamToFile(stream, _1BFile)
	if err != nil {
		panic(err)
	}

	fmt.Println("fromWebDavToS3 Done")
}

func fromS3ToWebDavPartially() {
	s3Store, _ := store.NewS3(store.S3Config{
		S3Bucket: _S3Bucket,
		Config: aws.Config{
			Region:      aws.String(_S3Region),
			Credentials: credentials.NewStaticCredentials(_S3AccessId, _S3AccessKey, _S3AccessToken),
			Endpoint:    aws.String(_S3Endpoint),
		},
	})

	webDavStore, _ := store.NewWebDav(store.WebDavConfig{
		WebDavHost: _WebDavHost,
		WebDavUser: _WebDavUser,
		WebDavPass: _WebDavPass,
	})

	uploadFn := func() error {
		_offset := int64(0)
		webdavSize := int64(0)

		webdavIsExist := webDavStore.IsExist(_500MBFileName)
		if webdavIsExist {
			webdavInfo, _, err := webDavStore.Stat(_500MBFileName)
			if err != nil {
				return err
			}
			webdavSize = webdavInfo.Size()
		} else {
			fmt.Println("file not exist")
		}

		s3Info, _, err := s3Store.Stat(_500MBFileName)
		if err != nil {
			return err
		}

		fmt.Println("webdavSize:", webdavSize/1024/1024, "MB", "/", s3Info.Size()/1024/1024, "MB")

		if webdavSize >= s3Info.Size() {
			return fmt.Errorf("file already uploaded")
		} else {
			// upload partially.
			_offset = webdavSize
		}

		stream, err := s3Store.FileReader(_500MBFileName, _offset, 0)
		if err != nil {
			return err
		}
		defer stream.Close()

		return webDavStore.StreamToFile(stream, _500MBFileName)
	}

	for {
		if err := uploadFn(); err != nil {
			if err.Error() == "file already uploaded" {
				break
			}
			panic(err)
		}

		time.Sleep(1 * time.Second) // wait for the file to be written
	}

	fmt.Println("fromS3ToWebDav Done")
}
