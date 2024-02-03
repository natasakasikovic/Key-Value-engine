package system

import (
	"errors"
	"fmt"
	config2 "github.com/natasakasikovic/Key-Value-engine/src/config"
	"github.com/natasakasikovic/Key-Value-engine/src/model"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/LRUCache"
	lsmtree "github.com/natasakasikovic/Key-Value-engine/src/structs/LSMTree"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/TokenBucket"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/WAL"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/memtable"
	"github.com/natasakasikovic/Key-Value-engine/src/structs/sstable"
	"log"
	"os"
	"strings"
	"time"
)

const (
	BF_KEY  = "bloomFilter"
	CMS_KEY = "countMinSketch"
	HLL_KEY = "hyperLogLog"
	SH_KEY  = "simhash"
	TB_KEY  = "tokenBucket"
)

type Engine struct {
	Wal         *WAL.WAL
	Cache       *LRUCache.LRUCache
	TokenBucket *TokenBucket.TokenBucket
	Config      *config2.Config
	LSMTree     *lsmtree.LSMTree
}

func NewEngine() (*Engine, error) {
	filePath := fmt.Sprintf("src%cconfig%cconfig.json", os.PathSeparator, os.PathSeparator)
	config, err := config2.LoadConfig(filePath)
	if err != nil {
		return nil, err
	}
	wal, err := WAL.NewWAL(config.WalSize, int32(config.MemtableSize))
	if err != nil {
		return nil, err
	}
	cache := LRUCache.NewLRUCache(config.LRUCacheMaxSize)

	memtable.InitMemtables(uint64(config.MemtableSize), config.MemtableStructure, uint64(config.MemTableMaxInstances), config.BTreeOrder)
	//Ucitavanje bf, cms, hll, simhash iz fajlova
	err = wal.ReadRecords()
	if err != nil {
		return nil, err
	}
	tokenBucket := TokenBucket.NewTokenBucket(config.NumberOfTokens, int64(config.TokenResetInterval))
	lsmTree := lsmtree.NewLSMTree(config)
	return &Engine{Wal: wal, Cache: cache, TokenBucket: tokenBucket, Config: config, LSMTree: &lsmTree}, nil
}

// Get Checks Memtable, Cache, BloomFilter and SSTable for given key
func (engine *Engine) Get(key string) ([]byte, error) {
	if strings.HasPrefix(key, BF_KEY) || strings.HasPrefix(key, CMS_KEY) || strings.HasPrefix(key, HLL_KEY) || strings.HasPrefix(key, SH_KEY) || strings.HasPrefix(key, TB_KEY) {
		return nil, errors.New("key must not begin with system prefix")
	}
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

	record, err := sstable.Search(key)
	if err == nil {
		value = record.Value
		engine.Cache.Add(key, value)
		return value, nil
	}
	return nil, nil
}

// Put Adds record to WAL and to Memtable with tombstone 0
func (engine *Engine) Put(key string, value []byte) error {
	if strings.HasPrefix(key, BF_KEY) || strings.HasPrefix(key, CMS_KEY) || strings.HasPrefix(key, HLL_KEY) || strings.HasPrefix(key, SH_KEY) || strings.HasPrefix(key, TB_KEY) {
		return errors.New("key must not begin with system prefix")
	}
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
	if strings.HasPrefix(key, BF_KEY) || strings.HasPrefix(key, CMS_KEY) || strings.HasPrefix(key, HLL_KEY) || strings.HasPrefix(key, SH_KEY) || strings.HasPrefix(key, TB_KEY) {
		return errors.New("key must not begin with system prefix")
	}
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
		sstable, err := sstable.CreateSStable(records, engine.Config.SSTableInSameFile, engine.Config.CompressionOn, int(engine.Config.IndexDegree), int(engine.Config.SummaryDegree))
		if err != nil {
			return err
		}
		engine.LSMTree.AddSSTable(sstable)
		engine.Cache.UpdateKeys(records)
	}
	return nil
}
