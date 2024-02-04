package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/bloomFilter"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/merkletree"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

// This method needs to read field by field in a block because we don't have that part of the file loaded into the operating memory,
// and we don't have information on how much we need to load.
func (sstable *SSTable) readNextIndex() ([]byte, int, error) {
	var err error
	var data []byte

	var keySizeBytes []byte = make([]byte, 8)
	_, err = io.ReadAtLeast(sstable.Index, keySizeBytes, 8)

	if err != nil {
		return nil, 0, err
	}

	keySize := binary.BigEndian.Uint64(keySizeBytes)
	var bufferKey []byte = make([]byte, keySize)
	_, err = io.ReadAtLeast(sstable.Index, bufferKey, int(keySize))

	if err != nil {
		return nil, 0, err
	}

	var offset []byte = make([]byte, 8)
	_, err = io.ReadAtLeast(sstable.Index, offset, 8)

	if err != nil {
		return nil, 0, err
	}

	data = append(data, keySizeBytes...)
	data = append(data, bufferKey...)
	data = append(data, offset...)

	return data, int(keySize), nil
}

// returns pointer to SSTable if succesfuly created, otherwise returns an error
func LoadSStableSingle(p string) (*SSTable, error) {
	var sstable *SSTable = &SSTable{}

	var path string = fmt.Sprintf("%s/%s%s", p, FILE_NAME, "DataIndexSummary.db")

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	sstable.Index, sstable.Data, sstable.Summary = file, file, file

	// set min key
	var minKeyLength uint64
	err = binary.Read(file, binary.BigEndian, &minKeyLength)
	if err != nil {
		file.Close()
		return nil, err
	}
	minKeyBytes := make([]byte, minKeyLength)
	_, err = file.Read(minKeyBytes)
	if err != nil {
		file.Close()
		return nil, err
	}
	sstable.MinKey = string(minKeyBytes)

	// set max key
	var maxKeyLength uint64
	err = binary.Read(file, binary.BigEndian, &maxKeyLength)
	if err != nil {
		file.Close()
		return nil, err
	}
	maxKeyBytes := make([]byte, maxKeyLength)
	_, err = file.Read(maxKeyBytes)
	if err != nil {
		file.Close()
		return nil, err
	}
	sstable.MaxKey = string(maxKeyBytes)

	// set offsets
	offsets := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		err = binary.Read(file, binary.BigEndian, &offsets[i])
		if err != nil {
			file.Close()
			return nil, err
		}
	}

	sstable.BfOffset, sstable.DataOffset, sstable.IndexOffset, sstable.SummaryOffset, sstable.MerkleOffset =
		int64(offsets[0]), int64(offsets[1]), int64(offsets[2]), int64(offsets[3]), int64(offsets[4])

	return sstable, nil
}

// returns pointer to SSTable if succesfuly created, otherwise returns an error
func LoadSSTableSeparate(path string) (*SSTable, error) {

	sstable := &SSTable{}
	path = fmt.Sprintf("%s/%s", path, FILE_NAME)

	// set summary file
	summaryFile, err := os.Open(fmt.Sprintf("%s%s", path, "Summary.db"))
	if err != nil {
		return nil, err
	}
	sstable.Summary = summaryFile

	// set min key
	var minKeyLength int64
	err = binary.Read(summaryFile, binary.BigEndian, &minKeyLength)
	if err != nil {
		summaryFile.Close()
		return nil, err
	}

	minKeyBytes := make([]byte, minKeyLength)
	_, err = summaryFile.Read(minKeyBytes)
	if err != nil {
		summaryFile.Close()
		return nil, err
	}

	sstable.MinKey = string(minKeyBytes)

	//set max key
	var maxKeyLength int64
	err = binary.Read(summaryFile, binary.BigEndian, &maxKeyLength)
	if err != nil {
		summaryFile.Close()
		return nil, err
	}

	maxKeyBytes := make([]byte, maxKeyLength)
	_, err = summaryFile.Read(maxKeyBytes)
	if err != nil {
		summaryFile.Close()
		return nil, err
	}

	sstable.MaxKey = string(maxKeyBytes)

	// set index file
	indexFile, err := os.Open(fmt.Sprintf("%s%s", path, "Index.db"))
	if err != nil {
		summaryFile.Close()
		return nil, err
	}
	sstable.Index = indexFile

	// set data file
	dataFile, err := os.Open(fmt.Sprintf("%s%s", path, "Data.db"))
	if err != nil {
		summaryFile.Close()
		indexFile.Close()
		return nil, err
	}
	sstable.Data = dataFile

	sstable.BfOffset, sstable.DataOffset, sstable.IndexOffset, sstable.MerkleOffset = 0, 0, 0, 0
	sstable.SummaryOffset = 2*8 + minKeyLength + maxKeyLength

	return sstable, nil
}

// params: bool singleFile - if we load from single file first read first 8 bytes to check size of bf
// if it is not single file then read all bytes from file
func (sstable *SSTable) loadBF(separateFile bool, path string) error {
	var file *os.File
	var err error
	var toRead []byte

	if separateFile {
		path = fmt.Sprintf("%s/%s/%s%s", PATH, path, FILE_NAME, "Filter.db")
		file, err = os.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		toRead, err = io.ReadAll(file)
		if err != nil {
			return err
		}
	} else {
		file = sstable.Data
		toRead = make([]byte, int(sstable.DataOffset-sstable.BfOffset))
		file.Seek(sstable.BfOffset, 0)
		_, err = io.ReadAtLeast(sstable.Data, toRead, len(toRead))
		if err != nil {
			return err
		}
	}

	sstable.Bf = bloomFilter.Deserialize(toRead)
	return nil
}

// param path: path to the folder where files will be saved;
// param records: array of pointers to records from memtable;
// param n: index degree, param m: summary degree;
// returns error: if it occured during actions connected to files;
func (sstable *SSTable) makeSeparateFiles(path string, records []*model.Record, n int, m int) error {

	data, _ := makeFile(path, "Data")
	summary, _ := makeFile(path, "Summary")
	index, _ := makeFile(path, "Index")
	filter, _ := makeFile(path, "Filter")
	merkle, _ := makeFile(path, "Metadata")
	if data != nil && index != nil && summary != nil {
		sstable.Data = data
		sstable.Index = index
		sstable.Summary = summary

		minKeyBytes := []byte(sstable.MinKey)
		maxKeyBytes := []byte(sstable.MaxKey)

		var minKeyLength uint64 = uint64(len(minKeyBytes))
		var maxKeyLength uint64 = uint64(len(maxKeyBytes))

		minKeyInfoSerialized := append(uint64ToBytes(minKeyLength), minKeyBytes...)
		maxKeyInfoSerialized := append(uint64ToBytes(maxKeyLength), maxKeyBytes...)

		var contentSummary [][]byte = [][]byte{minKeyInfoSerialized, maxKeyInfoSerialized}

		var contentData [][]byte = sstable.serializeData(records)
		var contentIndex [][]byte = sstable.serializeIndexSummary(contentData, n, sstable.CompressionOn)
		contentSummary = append(contentSummary, sstable.serializeIndexSummary(contentIndex, m, false)...)

		var contentBf [][]byte = [][]byte{sstable.Bf.Serialize()}

		var contentMerkle [][]byte = [][]byte{sstable.Merkle.Serialize()}

		sstable.writeToFile(data, contentData)
		sstable.writeToFile(index, contentIndex)
		sstable.writeToFile(summary, contentSummary)
		sstable.writeToFile(filter, contentBf)
		sstable.writeToFile(merkle, contentMerkle)
		return nil
	}
	return errors.New("error occured")
}

// function that calls every serialization
// saves header one after the other -> length of min key, so we know how we need to read to get min key
// same things is done for max key, then we saved offsets for data, index and summary
func (sstable *SSTable) writeToSingleFile(records []*model.Record, n int, m int) {
	var content [][]byte

	minKeyBytes := []byte(sstable.MinKey)
	maxKeyBytes := []byte(sstable.MaxKey)

	var minKeyLength uint64 = uint64(len(minKeyBytes))
	var maxKeyLength uint64 = uint64(len(maxKeyBytes))
	var bfOffset uint64 = 7*8 + minKeyLength + maxKeyLength
	var contentBf [][]byte = [][]byte{sstable.Bf.Serialize()}
	var dataOffset uint64 = calculateOffset(contentBf, bfOffset)
	var contentData [][]byte = sstable.serializeData(records)
	var indexOffset uint64 = calculateOffset(contentData, dataOffset)
	var contentIndex [][]byte = sstable.serializeIndexSummary(contentData, n, sstable.CompressionOn)
	var summaryOffset uint64 = calculateOffset(contentIndex, indexOffset)
	var contentSummary [][]byte = sstable.serializeIndexSummary(contentIndex, m, false)
	var merkleOffset uint64 = calculateOffset(contentSummary, summaryOffset)
	var contentMerkle [][]byte = [][]byte{sstable.Merkle.Serialize()}

	content = append(content, uint64ToBytes(minKeyLength))
	content = append(content, minKeyBytes)
	content = append(content, uint64ToBytes(maxKeyLength))
	content = append(content, maxKeyBytes)
	content = append(content, uint64ToBytes(bfOffset))
	content = append(content, uint64ToBytes(dataOffset))
	content = append(content, uint64ToBytes(indexOffset))
	content = append(content, uint64ToBytes(summaryOffset))
	content = append(content, uint64ToBytes(merkleOffset))
	content = append(content, contentBf...)
	content = append(content, contentData...)
	content = append(content, contentIndex...)
	content = append(content, contentSummary...)
	content = append(content, contentMerkle...)

	sstable.writeToFile(sstable.Data, content)
	sstable.DataOffset = int64(dataOffset)
	sstable.SummaryOffset = int64(summaryOffset)
	sstable.BfOffset = int64(bfOffset)
	sstable.MerkleOffset = int64(merkleOffset)
	sstable.IndexOffset = int64(indexOffset)
}

// params: n - index degree, m - summary degree
// makes sstable which is in single file, all *os.File in struct refer to same file
// this function also returns error if it occured during actions connected to files
func (sstable *SSTable) makeSingleFile(path string, records []*model.Record, n int, m int) error {
	file, err := makeFile(path, "DataIndexSummary")
	if err != nil {
		return err
	}
	sstable.Index, sstable.Data, sstable.Summary = file, file, file
	sstable.writeToSingleFile(records, n, m)
	return nil
}

// helper - makes files necessary for one sstable
// used in function makeSeparateFiles and makeSingleFile
func makeFile(path string, s string) (*os.File, error) {
	file, err := os.OpenFile(fmt.Sprintf("%s/%s%s.db", path, FILE_NAME, s), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// writes serialized content to file
func (sstable *SSTable) writeToFile(file *os.File, arr [][]byte) {
	var content []byte
	for i := 0; i < len(arr); i++ {
		content = append(content, arr[i]...)
	}
	file.Write(content)
	defer file.Close()
}

func (sstable *SSTable) loadMerkle(separateFile bool, path string) error {
	var file *os.File
	var err error
	var toRead []byte

	if separateFile {
		path = fmt.Sprintf("%s/%s/%s%s", PATH, path, FILE_NAME, "Metadata.db")
		file, err = os.Open(path)
		if err != nil {
			return err
		}
		toRead, err = io.ReadAll(file)
	} else {
		file = sstable.Data
		fileSize, err := utils.GetFileLength(file)
		if err != nil {
			return err
		}
		toRead = make([]byte, fileSize-sstable.MerkleOffset)
		file.Seek(sstable.MerkleOffset, 0)
		_, err = io.ReadAtLeast(sstable.Data, toRead, len(toRead))
		if err != nil {
			return nil
		}
	}

	sstable.Merkle = merkletree.Deserialize(toRead)
	return nil
}
