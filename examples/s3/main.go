package main

import (
	"flag"
	"fmt"
	"os"

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

	_500MBFile = "../files/test-500M.txt"
	_1BFile    = "../files/example.txt"
)

var choice int

func main() {
	s, _ := store.NewS3(store.S3Config{
		S3Bucket: _S3Bucket,
		Config: aws.Config{
			Region:      aws.String(_S3Region),
			Credentials: credentials.NewStaticCredentials(_S3AccessId, _S3AccessKey, _S3AccessToken),
			Endpoint:    aws.String(_S3Endpoint),
		},
	})

	flag.IntVar(&choice, "choice", 1, "1 - create, 2 - remove, 3 - stream to file, 4 - stat")
	flag.Parse()

	switch choice {
	case 1:
		create(s, "created.txt")
	case 2:
		remove(s, "created.txt")
	case 3:
		streamToFile(s, _500MBFile)
	case 4:
		stat(s, "created.txt")
	default:
		fmt.Println("Invalid choice")
	}
}

func create(s store.StoreIFace, file string) {
	err := s.CreateFile(
		file,
		[]byte("Hello, World!"),
		nil,
		map[string]string{
			"key1": "value1",
		})

	if err != nil {
		panic(err)
	}

	fmt.Println("Created")
}

func remove(s store.StoreIFace, file string) {
	err := s.RemoveFile(file)

	if err != nil {
		panic(err)
	}

	fmt.Println("Removed")
}

func streamToFile(s store.StoreIFace, file string) {
	stream, err := os.Open(file)
	if err != nil {
		panic(err)
	}

	err = s.StreamToFile(stream, "streamed.txt", nil)

	if err != nil {
		panic(err)
	}

	fmt.Println("Streamed to file")
}

func stat(s store.StoreIFace, file string) {
	_, meta, err := s.Stat(file)
	if err != nil {
		panic(err)
	}

	//fmt.Printf("Size: %d\n", info.Size()) // info.Size() is not available for S3
	fmt.Printf("Meta: %v\n", meta)
}
