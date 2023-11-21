package WAL

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	FILE_NAME            = "log_"
	MAX_RECORDS_PER_FILE = 1
)

type WAL struct {
	currentFile    *os.File
	currentSegment []*Record
	segmentNames   []string
}

func NewWAL() (*WAL, error) {

	files, err := os.ReadDir("log")
	if err != nil {
		return nil, err
	}
	var list []string
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
	currentSegment, err := GetRecordsFromFile(currentFile)
	if err != nil {
		return nil, err
	}
	return &WAL{currentFile: currentFile, currentSegment: currentSegment, segmentNames: list}, nil
}

func (wal *WAL) append(r *Record) error {
	data := r.RecordToBytes()
	//If current segment is full
	if len(wal.currentSegment) >= MAX_RECORDS_PER_FILE {
		err := wal.currentFile.Close()
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
		path := fmt.Sprintf("log%c%s%04d.log", os.PathSeparator, FILE_NAME, br+1)
		wal.currentFile, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
		if err != nil {
			return err
		}
		_, err = wal.currentFile.Write(r.RecordToBytes())
		if err != nil {
			return err
		}
		wal.segmentNames = append(wal.segmentNames, path) //Append current segment
		wal.currentSegment = make([]*Record, 0)           //Make new empty segment
	} else {
		_, err := wal.currentFile.Seek(0, 2) //Seek to EOF
		if err != nil {
			log.Fatal(err)
		}
		_, err = wal.currentFile.Write(data) //Write to the EOF
		if err != nil {
			return err
		}
	}
	wal.currentSegment = append(wal.currentSegment, r) //Apend current record current segment
	return nil
}
