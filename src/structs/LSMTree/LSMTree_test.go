package lsmtree

import (
	"testing"

	"github.com/natasakasikovic/Key-Value-engine/src/config"
	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/sstable"
)

// TestHelloEmpty calls greetings.Hello with an empty string,
// checking for an error.
func TestSvasta(t *testing.T) {
	conf := config.Config{
		WalSize:              3,
		MemtableSize:         3,
		MemtableStructure:    "skipList",
		MemTableMaxInstances: 3,
		SkipListMaxHeight:    3,
		BTreeOrder:           3,
		LRUCacheMaxSize:      5,
		IndexSummaryDegree:   5,
		SSTableInSameFile:    false,
		CompressionOn:        false,
		LSMTreeMaxDepth:      7,
		NumberOfTokens:       10,
		TokenResetInterval:   60,
	}

	var lsm_p *LSMTree = LoadLSMTreeFromFile(7, "leveled", 2, 10, 5, 5, false, false)

	var lsm LSMTree
	if lsm_p == nil {
		lsm = *NewLSMTree(7, "leveled", 2, 10, 5, 5, false, false)
	} else {
		lsm = *lsm_p
	}

	for i := 0; i < 7; i++ {
		records := []*model.Record{
			model.NewRecord(0, string('a'+rune(i*3)), []byte("a")),
			model.NewRecord(0, string('a'+rune(i*3+1)), []byte("b")),
			model.NewRecord(0, string('a'+rune(i*3+2)), []byte("c")),
		}
		newSSTable1, _ := sstable.CreateSStable(records, conf.SSTableInSameFile, conf.CompressionOn, int(conf.IndexSummaryDegree), int(conf.IndexSummaryDegree))
		lsm.AddSSTable(newSSTable1)
	}

	lsm.SaveToFile()

}
