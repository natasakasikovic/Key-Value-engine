package system

import (
	"errors"
	"log"
	"time"

	config2 "github.com/natasakasikovic/Key-Value-engine/src/config"
	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/LRUCache"
	lsmtree "github.com/natasakasikovic/Key-Value-engine/src/structs/LSMTree"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/TokenBucket"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/WAL"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/memtable"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/sstable"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"
)

const (
	BF_KEY  = "bloomFilter"
	CMS_KEY = "countMinSketch"
	HLL_KEY = "hyperLogLog"
	SH_KEY  = "simhash"
	TB_KEY  = "tokenBucket"
)

type Engine struct {
	Wal            *WAL.WAL
	Cache          *LRUCache.LRUCache
	TokenBucket    *TokenBucket.TokenBucket
	Config         *config2.Config
	LSMTree        *lsmtree.LSMTree
	CompressionMap map[string]uint64
}

func NewEngine() (*Engine, error) {
	filePath := "../src/config/config.json"
	config, err := config2.LoadConfig(filePath)
	if err != nil {
		return nil, err
	}

	// -------------------------------------------------------------------
	var dict map[string]uint64
	if config.CompressionOn {
		empty, err := utils.EmptyDir(sstable.COMPRESSION_PATH)
		if err != nil {
			return nil, err
		}
		if empty { // if the directory is empty, create a new hashmap and file

			file, err := sstable.MakeFile(sstable.COMPRESSION_PATH, "CompressionInfo") // TODO: possibly move this to a separate function, not on sstable

			if err != nil {
				return nil, err
			}

			defer file.Close()

			dict = make(map[string]uint64)
		} else { // load hashmap from file
			path := sstable.COMPRESSION_PATH + "/usertable-data-CompressionInfo.db"
			dict, err = sstable.LoadHashMap(path)

			if err != nil {
				return nil, err
			}

		}
	}
	//-------------------------------------------------------------------

	wal, err := WAL.NewWAL(config.WalSize, int32(config.MemTableMaxInstances))
	if err != nil {
		return nil, err
	}
	cache := LRUCache.NewLRUCache(config.LRUCacheMaxSize)

	memtable.InitMemtables(uint64(config.MemtableSize), config.MemtableStructure, uint64(config.MemTableMaxInstances), config.BTreeOrder, config.SkipListMaxHeight)
	//Ucitavanje bf, cms, hll, simhash iz fajlova
	err = wal.ReadRecords()
	if err != nil {
		return nil, err
	}
	tokenBucket := TokenBucket.NewTokenBucket(config.NumberOfTokens, int64(config.TokenResetInterval))

	tree, _ := lsmtree.LoadLSMTreeFromFile(config.LSMTreeMaxDepth, config.LSMCompactionType, config.LSMFirstLevelSize, config.LSMGrowthFactor, config.IndexDegree, config.SummaryDegree, config.SSTableInSameFile, config.CompressionOn)

	if tree == nil {
		tree, err = lsmtree.NewLSMTree(config.LSMTreeMaxDepth, config.LSMCompactionType, config.LSMFirstLevelSize, config.LSMGrowthFactor, config.IndexDegree, config.SummaryDegree, config.SSTableInSameFile, config.CompressionOn)
		if err != nil {
			return nil, err
		}
	}

	return &Engine{Wal: wal, Cache: cache, TokenBucket: tokenBucket, Config: config, LSMTree: tree, CompressionMap: dict}, nil
}

// Get Checks Memtable, Cache, BloomFilter and SSTable for given key
func (engine *Engine) Get(key string) ([]byte, error) {

	if !engine.TokenBucket.IsRequestAvailable() {
		return nil, errors.New("wait until sending new request")
	}
	memtableRecord, err := memtable.Get(key)
	if err == nil {
		return memtableRecord.Value, nil
	}
	value := engine.Cache.Get(key)
	if value != nil {
		return value, nil
	}

	record, err := sstable.Search(key, engine.CompressionMap)
	if err == nil {
		value = record.Value
		engine.Cache.Add(key, value)
		return value, nil
	}
	return nil, nil
}

// Put Adds record to WAL and to Memtable with tombstone 0
func (engine *Engine) Put(key string, value []byte) error {

	if !engine.TokenBucket.IsRequestAvailable() {
		return errors.New("wait until sending new request")
	}
	err := engine.Commit(key, value, 0)
	if err != nil {
		return err
	}
	return nil
}

// Delete Adds record to WAL and to Memtable with tombstone 1
func (engine *Engine) Delete(key string) error {
	if !engine.TokenBucket.IsRequestAvailable() {
		return errors.New("wait until sending new request")
	}
	err := engine.Commit(key, make([]byte, 0), 1)
	if err != nil {
		return err
	}
	return nil
}

func (engine *Engine) Commit(key string, value []byte, tombstone byte) error {
	timestamp := uint64(time.Now().UnixNano())
	r := model.NewRecordTimestamp(tombstone, key, value, timestamp)
	err := engine.Wal.Append(r)
	if err != nil {
		return err
	}
	didSwap, didFlush, records := memtable.Put(key, value, timestamp, tombstone)
	if didSwap {
		err = engine.Wal.UpdateWatermark(didFlush)
		if err != nil {
			log.Fatal(err)
		}
	}
	if didFlush {
		sstable, err := sstable.CreateSStable(records, engine.Config.SSTableInSameFile, engine.Config.CompressionOn, int(engine.Config.IndexDegree), int(engine.Config.SummaryDegree), engine.CompressionMap)
		if err != nil {
			return err
		}
		engine.LSMTree.AddSSTable(sstable)
		engine.Cache.UpdateKeys(records)
	}
	return nil
}
