package main

import (
	"fmt"

	"github.com/Citix-ltd/go-store"
)

func main() {
	s, _ := store.New(store.Config{
		StoreType:   store.LocalStore,
		LocalConfig: store.LocalConfig{},
	})

	fmt.Println("1 - create, 2 - remove, 3 - stream to file, 4 - stat")

	var choice int
	fmt.Scanln(&choice)

	switch choice {
	case 1:
		create(s, "store/created.txt")
	case 2:
		remove(s, "store/created.txt")
	case 3:
		streamToFile(s, "example.txt")
	case 4:
		stat(s, "store/created.txt")
	default:
		fmt.Println("Invalid choice")
	}
}

func create(s store.StoreIFace, file string) {
	err := s.CreateFile(
		file,
		[]byte("Hello, World!"), map[string]string{
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
	stream, err := s.FileReader(file, 0, 0)
	if err != nil {
		panic(err)
	}

	err = s.StreamToFile(stream, "store/streamed.txt")

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
