package main

import (
	"archive/zip"
	"fmt"
	"os"
	"path/filepath"
)

var Db = "SQLiteDB"

func validateFiles(filePath string) error {
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return fmt.Errorf("error checking file: %s", err)
	}
	//Check for empty files
	if fileInfo.Size() == 0 {
		return fmt.Errorf("file is empty")
	}
	if filepath.Ext(filePath) == ".tmp" || filepath.Ext(filePath) == ".temp" {
		return fmt.Errorf("temp file detected")
	}
	if err = isFileOpenable(filePath); err != nil {
		return fmt.Errorf("file can't be opened %s", err)
	}
	if err = checkForDuplicate(Db, filePath); err != nil {
		return fmt.Errorf("file already routed", err)
	}
	return nil
}
func isFileOpenable(filePath string) error {
	zipReader, err := zip.OpenReader(filePath)
	if err != nil {
		return err
	}
	defer zipReader.Close()
	return nil
}

func checkForDuplicate(db string, filePath string) error {
	// rows, err := db.Query("SELECT fileName from RoutedFiles")
	/*
		if err != nil{
			return fmt.Errorf("could not execute the query %s", err)
		}
		var fileName string
		for rows.Next(){
			err = rows.Scan(&fileName)
			if err!= nil{
				return fmt.Errorf("could not extract filename %s", err)
			}
		}
		if len(fileName) >0{
		return fmt.Errorf("file already routed")
		}

	*/
	fmt.Print(db, filePath)
	return nil
}
func main() {
	filePath := "/Users/chronon/Desktop/FB2.0-Go/test/test.txt"
	validateFiles(filePath)
}
