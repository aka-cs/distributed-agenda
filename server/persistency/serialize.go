package persistency

import (
	"encoding/gob"
	"log"
	"os"
	"path/filepath"
)

func Serialize[T any](object T, path string) {

	dataFile, err := os.Create(filepath.Join("resources", path))

	if err != nil {
		log.Fatalf("Error creating file:\n%v", err)
	}
	dataEncoder := gob.NewEncoder(dataFile)

	err = dataEncoder.Encode(object)
	if err != nil {
		log.Fatalf("Error serializing object:\n%v", err)
	}

	err = dataFile.Close()
	if err != nil {
		log.Fatalf("Error closing file:\n%v", err)
	}
}

func Deserialize[T any](path string) T {

	var result T

	dataFile, err := os.Open(filepath.Join("resources", path))

	if err != nil {
		log.Fatalf("Error opening file:\n%v", err)
	}

	dataDecoder := gob.NewDecoder(dataFile)
	err = dataDecoder.Decode(&result)

	if err != nil {
		log.Fatalf("Error deserializing object:\n%v", err)
	}

	err = dataFile.Close()

	if err != nil {
		log.Fatalf("Error closing file:\n%v", err)
	}

	return result
}
