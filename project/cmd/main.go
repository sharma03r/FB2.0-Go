package main

import (
	"errors"
	"fmt"
	"os"
)

func validateFiles(filePath string) error {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		fmt.Errorf("Error checking file:", err)
		return err
	}
	//Check for empty files
	if fileInfo.Size() == 0 {
		fmt.Errorf("File is empty")
		return errors.New("File is empty")
	}

	return nil
}

func main() {
	filePath := "/Users/chronon/Desktop/FB2.0-Go/test/test.txt"
	validateFiles(filePath)
}
