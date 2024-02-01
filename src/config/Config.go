package config

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	//"wal_size":5,"memtable_size":2,"memtable_structure":"skipList"
	WalSize              uint32 `json:"wal_size"`
	MemtableSize         uint32 `json:"memtable_size"`
	MemtableStructure    string `json:"memtable_structure"`
	MemTableMaxInstances uint32 `json:"memtable_max_instances"`
	SkipListMaxHeight    uint32 `json:"skip_list_max_height"`
	BTreeOrder           uint32 `json:"b_tree_order"`
	LRUCacheMaxSize      uint32 `json:"lru_cache_max_size"`
	IndexDegree          uint32 `json:"index_degree"`
	SummaryDegree        uint32 `json:"summary_degree"`
	SSTableInSameFile    bool   `json:"ss_table_in_same_file"`
	CompressionOn        bool   `json:"compression_on"`
	LSMTreeMaxDepth      uint32 `json:"lsm_tree_max_depth"`
	NumberOfTokens       uint32 `json:"number_of_tokens"`
	TokenResetInterval   uint32 `json:"token_reset_interval"`
	LSMFirstLevelSize    uint32 `json:"LSMFirstLevelSize"`
	LSMGrowthFactor      uint32 `json:"LSMGrowthFactor"`
	LSMCompactionType    string `json:"LSMCompactionType"`
}

func LoadConfig(filename string) (*Config, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	config := Config{
		WalSize:              3,
		MemtableSize:         3,
		MemtableStructure:    "skipList",
		MemTableMaxInstances: 3,
		SkipListMaxHeight:    3,
		BTreeOrder:           3,
		LRUCacheMaxSize:      5,
		IndexDegree:          5,
		SummaryDegree:        5,
		SSTableInSameFile:    false,
		CompressionOn:        false,
		LSMTreeMaxDepth:      7,
		NumberOfTokens:       10,
		TokenResetInterval:   60,
		LSMFirstLevelSize:    10,
		LSMGrowthFactor:      10,
		LSMCompactionType:    "sizetiered",
	}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}
