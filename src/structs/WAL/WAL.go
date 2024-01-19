package WAL

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

const (
	FILE_NAME = "log_"
)

type WAL struct {
	maxBytesPerFile      uint32
	currentFile          *os.File
	segmentNames         []string
	lowWaterMark         int
	bytesFromLastSegment int64
}

func getBytesFromLastSegmentFromFile() (int64, error) {
	path := fmt.Sprintf("src%cstructs%cWAL%cbytesFromLastSegment.log", os.PathSeparator, os.PathSeparator, os.PathSeparator)

	file, err := os.Open(path)
	if os.IsNotExist(err) {
		file, err = os.Create(path)
		if err != nil {
			return -1, err
		}
	} else if err != nil {
		return -1, err
	}

	defer file.Close()
	fileLength, err := getFileLength(file)
	if err != nil {
		return -1, err
	}
	if fileLength == 0 {
		return 0, nil
	}
	buf := make([]byte, 8)
	_, err = file.Read(buf)
	if err != nil {
		return -1, err
	}

	value := int64(binary.LittleEndian.Uint64(buf))

	return value, nil
}
func setBytesFromLastSegmentFromFile(value int64) error {
	path := fmt.Sprintf("src%cstructs%cWAL%cbytesFromLastSegment.log", os.PathSeparator, os.PathSeparator, os.PathSeparator)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Convert int64 to byte slice
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(value))

	// Write the byte slice to the file
	_, err = file.Write(buf)
	if err != nil {
		return err
	}

	return nil
}
func NewWAL(maxBytesPerFile uint32) (*WAL, error) {

	files, err := os.ReadDir("log")
	if os.IsNotExist(err) {
		err := os.Mkdir("log", os.ModeDir)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
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
	currentFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(path, err)
		return nil, err
	}
	bytesFromLastSegment, err := getBytesFromLastSegmentFromFile()
	if err != nil {
		return nil, err
	}
	return &WAL{
		maxBytesPerFile:      maxBytesPerFile,
		currentFile:          currentFile,
		segmentNames:         list,
		lowWaterMark:         1,
		bytesFromLastSegment: bytesFromLastSegment}, nil
}
func (wal *WAL) Commit(key string, value []byte, tombstone byte) {
	err := wal.Append(NewRecord(tombstone, key, value))
	if err != nil {
		return
	}
}
func (wal *WAL) Append(r *Record) error {
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
		fileName := fmt.Sprintf("%s%04d.log", FILE_NAME, br+1)
		wal.segmentNames = append(wal.segmentNames, fileName)
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
	didBreak := false
	for i, fileName := range wal.segmentNames {
		file, err := os.OpenFile("log"+string(os.PathSeparator)+fileName, os.O_RDONLY, 644)
		if err != nil {
			return err
		}
		//Skip bytes from last file
		if i == 0 {
			_, err := file.Seek(wal.bytesFromLastSegment, 0)
			if err != nil {
				return err
			}
		}
		content, err := io.ReadAll(file)
		if err != nil {
			return err
		}
		data := append(bytesToTransfer, content...)
		for offset := 0; offset < len(data); {
			bytesLeft := uint32(len(data)) - uint32(offset)
			//Ako je ostalo manje od 29 bajtova, ne mozemo ni celu duzinu procitati, otvaraj novi
			if bytesLeft < 29 {
				bytesToTransfer = make([]byte, bytesLeft)
				copy(bytesToTransfer, data[offset:])
				err := file.Close()
				if err != nil {
					return err
				}
				didBreak = true
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
					didBreak = true
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
					didBreak = false
				}
			}
		}
		if !didBreak {
			err = file.Close()
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	return nil
}

func (wal *WAL) ClearLog() error {
	for i := 0; i < wal.lowWaterMark-1; i++ {
		err := os.Remove("log" + string(os.PathSeparator) + wal.segmentNames[i])
		if err != nil {
			return err
		}
	}
	newSegmentNames := make([]string, len(wal.segmentNames)-wal.lowWaterMark+1)
	err := wal.currentFile.Close()
	if err != nil {
		return err
	}
	for i := wal.lowWaterMark - 1; i < len(wal.segmentNames); i++ {
		num := i - wal.lowWaterMark + 2
		newName := fmt.Sprintf("%s%04d.log", FILE_NAME, num)
		oldName := wal.segmentNames[i]
		oldPath := "log" + string(os.PathSeparator) + oldName
		newPath := "log" + string(os.PathSeparator) + newName
		err := os.Rename(oldPath, newPath)
		if err != nil {
			log.Fatal(err)
			return err
		}
		newSegmentNames[i-wal.lowWaterMark+1] = newName
	}
	wal.currentFile, err = os.OpenFile("log"+string(os.PathSeparator)+newSegmentNames[len(newSegmentNames)-1], os.O_RDWR|os.O_CREATE, 0644)
	wal.segmentNames = newSegmentNames
	fmt.Println(wal.segmentNames)

	return nil
}

func (wal *WAL) UpdateWatermark() error {
	wal.lowWaterMark = len(wal.segmentNames)
	fileLength, err := getFileLength(wal.currentFile)
	if err != nil {
		return err
	}
	wal.bytesFromLastSegment = fileLength
	err = setBytesFromLastSegmentFromFile(fileLength)
	if err != nil {
		return err
	}
	//err = wal.ClearLog()
	return nil
}

func getFileLength(f *os.File) (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}
