package sstable

import (
	"os"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/bloomFilter"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/merkletree"
)

type SSTable struct {
	data         *os.File
	index        *os.File
	indexSummary *os.File
	bf           *bloomFilter.BloomFilter
	merkle       *merkletree.MerkleTree
}

func CreateSStable(records []*model.Record) *SSTable { // TODO forward config

	sstable := SSTable{}

	if true { // if config.SSTableInSameFile is true

	} else {

	}

	sstable.bf = bloomFilter.NewBf(len(records), 0.001)

	var content [][]byte
	for _, record := range records {
		content = append(content, record.ToBytes())
	}

	sstable.merkle, _ = merkletree.NewTree(content)
	return &sstable
}
