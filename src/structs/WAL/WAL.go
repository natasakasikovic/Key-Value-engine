package WAL

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/memtable"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

const (
	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
	//keySize := binary.BigEndian.Uint64(data[KEY_SIZE_START : KEY_SIZE_START+KEY_SIZE_SIZE])
	//valueSize := binary.BigEndian.Uint64(data[VALUE_SIZE_START : VALUE_SIZE_START+VALUE_SIZE_SIZE])
)
const (
	FILE_NAME = "log_"
)

type WAL struct {
	maxBytesPerFile              uint32
	currentFile                  *os.File
	segmentNames                 []string
	lowWaterMark                 int32
	bytesFromLastSegment         int64
	numOfMemtables               int32
	currentMemtable              int32
	memtableLowWatermark         []int32
	memtableBytesFromLastSegment []int64
}

func getBytesFromLastSegmentFromFile(numOfMemtables int32) (int64, int32, []int64, []int32, int32, error) {
	path := "structs/WAL/bytesFromLastSegment.log"

	file, err := os.Open(path)
	if os.IsNotExist(err) {
		file, err = os.Create(path)
		if err != nil {
			return -1, -1, nil, nil, -1, err
		}
	} else if err != nil {
		return -1, -1, nil, nil, -1, err
	}

	defer file.Close()
	fileLength, err := utils.GetFileLength(file)
	if err != nil {
		return -1, -1, nil, nil, -1, err
	}
	if fileLength == 0 {
		return 0, 1, make([]int64, numOfMemtables), make([]int32, numOfMemtables), 0, err
	}
	size := numOfMemtables*(4+8) + 4
	buf := make([]byte, size)
	_, err = file.Read(buf)
	if err != nil {
		return -1, -1, nil, nil, -1, err
	}
	memtableBytesFromLastSegment := make([]int64, numOfMemtables)
	memtableLowWatermark := make([]int32, numOfMemtables)
	for i := int32(0); i < numOfMemtables; i++ {
		memtableBytesFromLastSegment[i] = int64(binary.LittleEndian.Uint64(buf[i*8 : i*8+8]))
	}
	for i := int32(0); i < numOfMemtables; i++ {
		memtableLowWatermark[i] = int32(binary.LittleEndian.Uint32(buf[numOfMemtables*8+i*4 : numOfMemtables*8+i*4+4]))
	}
	currentMemtable := int32(binary.LittleEndian.Uint32(buf[(8+4)*numOfMemtables : (8+4)*numOfMemtables+4]))

	bytesFromLastSegment := memtableBytesFromLastSegment[currentMemtable]
	lowWaterMark := memtableLowWatermark[currentMemtable]
	return bytesFromLastSegment, lowWaterMark, memtableBytesFromLastSegment, memtableLowWatermark, currentMemtable, nil
}
func (wal *WAL) SetBytesFromLastSegmentFromFile() error {
	path := "structs/WAL/bytesFromLastSegment.log"
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	size := wal.numOfMemtables*(4+8) + 4
	buf := make([]byte, size)
	for i := int32(0); i < wal.numOfMemtables; i++ {
		binary.LittleEndian.PutUint64(buf[i*8:i*8+8], uint64(wal.memtableBytesFromLastSegment[i]))
	}
	for i := int32(0); i < wal.numOfMemtables; i++ {
		binary.LittleEndian.PutUint32(buf[wal.numOfMemtables*8+i*4:wal.numOfMemtables*8+i*4+4], uint32(wal.memtableLowWatermark[i]))
	}
	binary.LittleEndian.PutUint32(buf[(8+4)*wal.numOfMemtables:(8+4)*wal.numOfMemtables+4], uint32(wal.currentMemtable))

	// Write the byte slice to the file
	_, err = file.Write(buf)
	if err != nil {
		return err
	}

	return nil
}
func NewWAL(maxBytesPerFile uint32, numOfMemtables int32) (*WAL, error) {

	files, err := os.ReadDir("../data/log")
	if os.IsNotExist(err) {
		err := os.Mkdir("../data/log", os.ModeDir)
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
		path = fmt.Sprintf("../data/log/%s%s.log", FILE_NAME, "0001")
		list = append(list, fmt.Sprintf("%s%s.log", FILE_NAME, "0001"))
	} else {
		path = fmt.Sprintf("../data/log/%s", list[len(list)-1])
	}
	currentFile, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		fmt.Println(path, err)
		return nil, err
	}
	bytesFromLastSegment, watermark, mBytesFromLastSegment, mWatermark, currentMemtable, err := getBytesFromLastSegmentFromFile(numOfMemtables)
	if err != nil {
		return nil, err
	}
	return &WAL{
		maxBytesPerFile:              maxBytesPerFile,
		currentFile:                  currentFile,
		segmentNames:                 list,
		lowWaterMark:                 watermark,
		numOfMemtables:               numOfMemtables,
		bytesFromLastSegment:         bytesFromLastSegment,
		memtableBytesFromLastSegment: mBytesFromLastSegment,
		memtableLowWatermark:         mWatermark,
		currentMemtable:              currentMemtable}, nil
}
func (wal *WAL) Commit(key string, value []byte, tombstone byte) {
	err := wal.Append(model.NewRecord(tombstone, key, value))
	if err != nil {
		return
	}
}
func (wal *WAL) Append(r *model.Record) error {
	data := r.RecordToBytes()
	fileLength, err := utils.GetFileLength(wal.currentFile)
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
		path := fmt.Sprintf("../data/log/%s%04d.log", FILE_NAME, br+1) // making next file
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
	for i, fileName := range wal.segmentNames {
		path := fmt.Sprintf("../data/log/%s", fileName)
		file, err := os.OpenFile(path, os.O_RDONLY, 644)
		defer file.Close()
		if err != nil {
			return err
		}
		//Skip bytes from last file
		temp := fileName[4:8]
		intNumber, err := strconv.Atoi(temp)
		if err != nil {
			return err
		}
		if int32(intNumber) < wal.lowWaterMark {
			continue
		}
		if int32(i) == wal.lowWaterMark-1 {
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
				if err != nil {
					return err
				}
				break
			} else {
				keySize := binary.BigEndian.Uint64(data[offset+KEY_SIZE_START : offset+KEY_SIZE_START+KEY_SIZE_SIZE])
				valueSize := binary.BigEndian.Uint64(data[offset+VALUE_SIZE_START : offset+VALUE_SIZE_START+VALUE_SIZE_SIZE])
				//Sad kad znamo celu duzinu ako je ostalo vise bajtova nego duzina recorda opet otvaraj novi
				if uint64(bytesLeft) < 29+keySize+valueSize {
					bytesToTransfer = make([]byte, bytesLeft)
					copy(bytesToTransfer, data[offset:])
					if err != nil {
						return err
					}
					break
				} else { //U suprotnom ucitaj record
					record, bytesRead, err := model.ReadSingleRecord(data[offset:])
					if err != nil {
						return err
					}
					//Read records 1 by 1
					//IF THERE IS ENOUGH TO FLUSH IT WOULD'VE BEEN FLUSHED EARLIER
					didSwap, _, _ := memtable.Put(record.Key, record.Value, record.Timestamp, record.Tombstone)
					if didSwap {
						err := wal.UpdateWatermark(false)
						if err != nil {
							return err
						}
					}
					fmt.Println(record)

					offset += bytesRead
				}
			}
		}
	}
	return nil
}

func (wal *WAL) ClearLog() error {
	for i := int32(0); i < wal.lowWaterMark-1; i++ {
		err := os.Remove("data" + string(os.PathSeparator) + "log" + string(os.PathSeparator) + wal.segmentNames[i])
		if err != nil {
			return err
		}
	}
	newSegmentNames := make([]string, int32(len(wal.segmentNames))-wal.lowWaterMark+1)
	err := wal.currentFile.Close()
	if err != nil {
		return err
	}
	for i := wal.lowWaterMark - 1; i < int32(len(wal.segmentNames)); i++ {
		num := i - wal.lowWaterMark + 2
		newName := fmt.Sprintf("%s%04d.log", FILE_NAME, num)
		oldName := wal.segmentNames[i]
		oldPath := "data" + string(os.PathSeparator) + "log" + string(os.PathSeparator) + oldName
		newPath := "data" + string(os.PathSeparator) + "log" + string(os.PathSeparator) + newName
		err := os.Rename(oldPath, newPath)
		if err != nil {
			log.Fatal(err)
			return err
		}
		newSegmentNames[i-wal.lowWaterMark+1] = newName
	}
	wal.lowWaterMark = 1
	wal.memtableLowWatermark[wal.currentMemtable] = 1
	wal.bytesFromLastSegment = wal.memtableBytesFromLastSegment[wal.currentMemtable]
	err = wal.SetBytesFromLastSegmentFromFile()
	if err != nil {
		return err
	}
	wal.currentFile, err = os.OpenFile("data"+string(os.PathSeparator)+"log"+string(os.PathSeparator)+newSegmentNames[len(newSegmentNames)-1], os.O_RDWR|os.O_CREATE, 0644)
	wal.segmentNames = newSegmentNames
	fmt.Println(wal.segmentNames)

	return nil
}

func (wal *WAL) UpdateWatermark(didFlush bool) error {
	wal.memtableLowWatermark[wal.currentMemtable] = int32(len(wal.segmentNames))
	fileLength, err := utils.GetFileLength(wal.currentFile)
	if err != nil {
		return err
	}
	wal.memtableBytesFromLastSegment[wal.currentMemtable] = fileLength

	wal.currentMemtable = (wal.currentMemtable + 1) % wal.numOfMemtables
	if didFlush {
		wal.lowWaterMark = wal.memtableLowWatermark[wal.currentMemtable]
		wal.bytesFromLastSegment = wal.memtableBytesFromLastSegment[wal.currentMemtable]
	}

	err = wal.SetBytesFromLastSegmentFromFile()
	if err != nil {
		return err
	}
	return nil
}
