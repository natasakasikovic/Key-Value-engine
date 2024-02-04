package utils

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// returns list of strings which represents name of content
// used in sstable and wal
func GetDirContent(path string) ([]string, error) {
	dirs, err := os.ReadDir(path)

	if os.IsNotExist(err) {
		err := os.Mkdir(path, os.ModeDir)
		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	var contentNames []string
	for _, name := range dirs {
		contentNames = append(contentNames, name.Name())
	}
	return contentNames, nil
}

func GetNextContentName(contentName []string, path string, s string) ([]string, string, error) {
	name := contentName[len(contentName)-1]
	temp := strings.Split(name, "_")[1]
	brStringUnCut := strings.Split(temp, ".")[0]
	brString := strings.TrimLeft(brStringUnCut, "0")
	br, err := strconv.Atoi(brString)
	if err != nil {
		return nil, "", err
	}
	path = fmt.Sprintf("%s/%s%04d", path, s, br+1)
	fileName := fmt.Sprintf("%s%04d", s, br+1)
	contentName = append(contentName, fileName)

	return contentName, path, nil
}

func GetFileLength(f *os.File) (int64, error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

// reads a variable-length encoded uint64 from the given file
// used in compression
func ReadUvarint(file *os.File) (uint64, uint64, error) {
	var valueBuf [binary.MaxVarintLen64]byte
	n, err := file.Read(valueBuf[:])
	if err != nil {
		return 0, 0, err
	}
	value, bytesRead := binary.Uvarint(valueBuf[:n])
	file.Seek(int64(bytesRead-n), 1)
	return value, uint64(bytesRead), nil
}

// encodes the given uint64 value into variable-length format and writes it to the provided buffer.
// used in compression
func PutUvarint(buf *bytes.Buffer, value uint64) {
	var uvarintBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(uvarintBuf[:], value)
	buf.Write(uvarintBuf[:n])
}

// checks if folder is empty
func EmptyDir(path string) (bool, error) {
	dirContent, err := GetDirContent(path)
	if err != nil {
		return false, err
	}
	return len(dirContent) == 0, nil
}

func GetKeyByValue(compressedKey uint64, compressionMap map[string]uint64) string {
	for key, value := range compressionMap {
		if value == compressedKey {
			return key
		}
	}
	return ""
}
