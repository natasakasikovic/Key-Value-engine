package utils

import (
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
