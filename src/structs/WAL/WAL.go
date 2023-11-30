package WAL

import (
	"encoding/binary"
	"fmt"
	"io/ioutil"
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
	fileLength, err := getFileLength(wal.currentFile)
	if err != nil {
		return err
	}
	bytesLeft := int64(wal.maxBytesPerFile) - fileLength // number of left bytes
	if bytesLeft >= int64(len(data)) {                   // if there is enough space for record, just write it
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

func (wal *WAL) ReadRecords() error {
	bytesToTransfer := make([]byte, 0)
	for _, fileName := range wal.segmentNames {
		file, err := os.OpenFile(fileName, os.O_RDONLY, 644)
		if err != nil {
			return err
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}
		data := append(bytesToTransfer, content...)
		for offset := 0; offset < len(data); {
			bytesLeft := wal.maxBytesPerFile - uint32(offset)
			//Ako je ostalo manje od 29 bajtova, ne mozemo ni celu duzinu procitati, otvaraj novi
			if bytesLeft < 29 {
				bytesToTransfer = make([]byte, bytesLeft)
				copy(bytesToTransfer, data[offset:])
				err := file.Close()
				if err != nil {
					return err
				}
				break
			} else {
				keySize := binary.BigEndian.Uint64(data[KEY_SIZE_START : KEY_SIZE_START+KEY_SIZE_SIZE])
				valueSize := binary.BigEndian.Uint64(data[VALUE_SIZE_START : VALUE_SIZE_START+VALUE_SIZE_SIZE])
				//Sad kad znamo celu duzinu ako je ostalo vise bajtova nego duzina recorda opet otvaraj novi
				if uint64(bytesLeft) < 29+keySize+valueSize {
					bytesToTransfer = make([]byte, bytesLeft)
					copy(bytesToTransfer, data[offset:])
					err := file.Close()
					if err != nil {
						return err
					}
					break
				} else { //U suprotnom ucitaj record
					record, bytesRead, err := ReadSingleRecord(data[offset:])
					if err != nil {
						return err
					}
					//Citamo recorde 1 po 1
					//Kada se podaci iz memtabele izgube, ovde citamo 1 po 1 zapis
					//I na osnovu zapisa ponovo popunjavamo novu memtabelu
					//npr memtable.add (record.key, record.value) ili memtable.delete(record.key) ako je tombstone != 0
					fmt.Println(record)
					offset += bytesRead
				}
			}
		}
	}
	return nil
}

func getFileLength(f *os.File) (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
