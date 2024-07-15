package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/Citix-ltd/go-store"
)

const (
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
	s, _ := store.NewWebDav(store.WebDavConfig{
		WebDavHost: _WebDavHost,
		WebDavUser: _WebDavUser,
		WebDavPass: _WebDavPass,
	})

	flag.IntVar(&choice, "choice", 1, "1 - create, 2 - remove, 3 - stream to file, 4 - stat")
	flag.Parse()

	switch choice {
	case 1:
		create(s, "created.txt")
	case 2:
		remove(s, _500MBFileName)
	case 3:
		streamToFile(s, _1BFile)
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
		},
	)

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
	info, meta, err := s.Stat(file)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Size: %d\n", info.Size())
	fmt.Printf("Meta: %v\n", meta)
}
