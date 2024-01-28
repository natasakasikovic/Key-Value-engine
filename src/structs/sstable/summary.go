package sstable

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
