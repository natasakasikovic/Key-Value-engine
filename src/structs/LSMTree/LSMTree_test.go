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

	var lsm_p *LSMTree = LoadLSMTreeFromFile(7, "sizetiered", 10, 10, 5, 5, true, false)

	var lsm LSMTree
	if lsm_p == nil {
		lsm = *NewLSMTree(7, "sizetiered", 10, 10, 5, 5, true, false)
	} else {
		lsm = *lsm_p
	}

	var records1 []*model.Record = []*model.Record{
		model.NewRecord(0, "a", []byte("a")),
		model.NewRecord(0, "b", []byte("b")),
		model.NewRecord(0, "c", []byte("c")),
	}

	var records2 []*model.Record = []*model.Record{
		model.NewRecord(0, "d", []byte("a")),
		model.NewRecord(0, "e", []byte("b")),
		model.NewRecord(0, "f", []byte("c")),
	}

	var records3 []*model.Record = []*model.Record{
		model.NewRecord(0, "g", []byte("a")),
		model.NewRecord(0, "h", []byte("b")),
		model.NewRecord(0, "i", []byte("c")),
	}

	var wait int = 0
	for i := 0; i < 10000000; i++ {
		wait += 1
	}

	var records4 []*model.Record = []*model.Record{
		model.NewRecord(0, "a", []byte("a")),
		model.NewRecord(0, "i", []byte("b")),
	}

	newSSTable1, _ := sstable.CreateSStable(records1, conf.SSTableInSameFile, conf.CompressionOn, int(conf.IndexSummaryDegree), int(conf.IndexSummaryDegree))
	lsm.AddSSTable(newSSTable1)
	newSSTable2, _ := sstable.CreateSStable(records2, conf.SSTableInSameFile, conf.CompressionOn, int(conf.IndexSummaryDegree), int(conf.IndexSummaryDegree))
	lsm.AddSSTable(newSSTable2)
	newSSTable4, _ := sstable.CreateSStable(records4, conf.SSTableInSameFile, conf.CompressionOn, int(conf.IndexSummaryDegree), int(conf.IndexSummaryDegree))
	lsm.AddSSTable(newSSTable4)
	newSSTable3, _ := sstable.CreateSStable(records3, conf.SSTableInSameFile, conf.CompressionOn, int(conf.IndexSummaryDegree), int(conf.IndexSummaryDegree))
	lsm.AddSSTable(newSSTable3)

	lsm.SaveToFile()

}
