package sstable

import "io"

// returns 2 offsets between which we should search index
// if offset2 == 0, then search until the end of index
func (sstable *SSTable) searchSummary(data []byte, key string) (uint64, uint64) {
	var prev, next string
	var offset1, offset2 uint64
	var bytesRead int

	prev, offset1, bytesRead = readBlock(data)
	data = data[bytesRead:]
	for len(data) > 0 {
		next, offset2, bytesRead = readBlock(data)
		data = data[bytesRead:] // remove bytes that we have read
		if key >= prev && key < next {
			break
		}
		prev = next
		offset1 = offset2
	}
	if key > next { // if key is greater that last exisitng key in summary, then read index [offset1:]
		return offset1, 0
	}
	_, _, bytesRead = readBlock(data)
	return offset1, offset2 + uint64(bytesRead) // read also next one
}

// in summary
// loads summary and returns bytes loaded from file, otherwise returns an error
func (sstable *SSTable) loadSummary(separateFile bool) ([]byte, error) {
	var content []byte
	var err error

	if separateFile {
		_, err = sstable.summary.Seek(sstable.summaryOffset, 0) // in separate files there are headers written, so seek
		if err != nil {
			return nil, err
		}
		content, err = io.ReadAll(sstable.summary)
		if err != nil {
			return nil, err
		}
	} else {
		// TODO: check
		// var toReadLength int = int(sstable.summaryOffset - sstable.bfOffset)
		// sstable.summary.Seek(sstable.summaryOffset, 0)
		// _, err = io.ReadAtLeast(sstable.summary, data, toReadLength)
	}
	return content, nil
}
