package WAL

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	FILE_NAME = "log_"
)

type WAL struct {
	maxBytesPerFile uint32
	currentFile     *os.File
	segmentNames    []string
}

func NewWAL(maxBytesPerFile uint32) (*WAL, error) {

	files, err := os.ReadDir("log")
	if err != nil {
		return nil, err
	}
	var list []string // list of file names
	for _, file := range files {
		list = append(list, file.Name()) //List of files
	}
	var path string
	//If there are no files
	if len(list) == 0 {
		path = fmt.Sprintf("log%c%s%s.log", os.PathSeparator, FILE_NAME, "0001")
		list = append(list, fmt.Sprintf("%s%s.log", FILE_NAME, "0001"))
	} else {
		path = fmt.Sprintf("log%c%s", os.PathSeparator, list[len(list)-1])
	}
	fmt.Println(path)
	currentFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(path, err)
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return &WAL{
		maxBytesPerFile: maxBytesPerFile,
		currentFile:     currentFile,
		segmentNames:    list}, nil
}

func (wal *WAL) append(r *Record) error {
	data := r.RecordToBytes()
	bytesLeft := int64(wal.maxBytesPerFile) - getFileLength(wal.currentFile) // number of left bytes
	if bytesLeft >= int64(len(data)) {                                       // if there is enough space for record, just write it
		_, err := wal.currentFile.Seek(0, 2) //Seek to EOF
		if err != nil {
			log.Fatal(err)
		}
		_, err = wal.currentFile.Write(data) //Write to the EOF
		if err != nil {
			return err
		}
	} else { // if current file is full
		dataCurrentFile := data[:bytesLeft]
		dataNextFile := data[bytesLeft:]
		_, err := wal.currentFile.Seek(0, 2) //Seek to EOF
		if err != nil {
			log.Fatal(err)
		}
		_, err = wal.currentFile.Write(dataCurrentFile) //Write to the EOF
		if err != nil {
			return err
		}

		err = wal.currentFile.Close()
		if err != nil {
			return err
		}

		name := wal.segmentNames[len(wal.segmentNames)-1] //Segment name: log_0001.log
		temp := strings.Split(name, "_")[1]               //0001.log
		brStringUnCut := strings.Split(temp, ".")[0]      //0001
		brString := strings.TrimLeft(brStringUnCut, "0")  //1
		br, err := strconv.Atoi(brString)                 //string to int
		if err != nil {
			return err
		}
		path := fmt.Sprintf("log%c%s%04d.log", os.PathSeparator, FILE_NAME, br+1) // making next file
		wal.currentFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		_, err = wal.currentFile.Write(dataNextFile)
		if err != nil {
			return err
		}
	}
	return nil
}

func getFileLength(f *os.File) int64 {
	fi, err := f.Stat()
	if err != nil {

	}
	return fi.Size()
}
