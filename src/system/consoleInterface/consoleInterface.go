package consoleinterface

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/natasakasikovic/Key-Value-engine/src/structs/sstable"
	"github.com/natasakasikovic/Key-Value-engine/src/utils"

	"github.com/natasakasikovic/Key-Value-engine/src/model"
	countMinSketch "github.com/natasakasikovic/Key-Value-engine/src/structs/CountMinSketch"
	hyperLogLog "github.com/natasakasikovic/Key-Value-engine/src/structs/HyperLogLog"
	bloomFilter "github.com/natasakasikovic/Key-Value-engine/src/structs/bloomFilter"
	iterators "github.com/natasakasikovic/Key-Value-engine/src/structs/iterators"
	scan "github.com/natasakasikovic/Key-Value-engine/src/structs/scan"
	simHash "github.com/natasakasikovic/Key-Value-engine/src/structs/simHash"
	engine "github.com/natasakasikovic/Key-Value-engine/src/system"
)

const (
	BF_KEY  = "bloomFilter"
	CMS_KEY = "countMinSketch"
	HLL_KEY = "hyperLogLog"
	SH_KEY  = "simhash"
	TB_KEY  = "tokenBucket"
)

func prefixScan(isSStableCompressed bool, prefix string, compressionMap map[string]uint64) ([]*model.Record, error) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("Enter the page number: ")
	scanner.Scan()
	pgNum := scanner.Text()

	pgNumber, err := strconv.Atoi(strings.TrimSpace(pgNum))
	if err != nil {
		fmt.Println("Wrong input. Please try again.")
		return nil, err
	}

	fmt.Print("Enter the page size: ")
	scanner.Scan()
	pgSize := scanner.Text()

	pageSize, err := strconv.Atoi(strings.TrimSpace(pgSize))
	if err != nil {
		fmt.Println("Wrong input. Please try again.")
		return nil, err
	}

	records, err := scan.PrefixScan(prefix, pgNumber, pageSize, isSStableCompressed, compressionMap)
	if err == nil {
		return records, err
	} else {
		return nil, err
	}

}
func rangeScan(isSStableCompressed bool, compressionMap map[string]uint64) ([]*model.Record, error) {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print("From: ")
	scanner.Scan()
	minKey := scanner.Text()

	fmt.Print("To: ")
	scanner.Scan()
	maxKey := scanner.Text()

	fmt.Print("Enter the page number: ")
	scanner.Scan()
	pgNum := scanner.Text()

	pgNumber, err := strconv.Atoi(strings.TrimSpace(pgNum))
	if err != nil {
		fmt.Println("Wrong input. Please try again.")
		return nil, err
	}

	fmt.Print("Enter the page size: ")
	scanner.Scan()
	pgSize := scanner.Text()

	pageSize, err := strconv.Atoi(strings.TrimSpace(pgSize))
	if err != nil {
		fmt.Println("Wrong input. Please try again.")
		return nil, err
	}

	records, err := scan.RangeScan(minKey, maxKey, pgNumber, pageSize, isSStableCompressed, compressionMap)
	if err == nil {
		return records, err
	} else {
		return nil, err
	}
}
func prefixIterate(isSStableCompressed bool, compressionMap map[string]uint64) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("Enter the prefix: ")
	scanner.Scan()
	prefix := scanner.Text()
	prefixIterator, err := iterators.NewPrefixIterator(prefix, isSStableCompressed, compressionMap)
	if prefixIterator == nil && err != nil {
		fmt.Printf("err: %v\n", err)
		return
	} else if prefixIterator != nil && err == nil {
		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Println("1 --> Next")
			fmt.Println("2 --> Stop")
			scanner.Scan()
			input := scanner.Text()

			option, err := strconv.Atoi(strings.TrimSpace(input))
			if err != nil {
				fmt.Println("Wrong input. Please try again.")
				continue
			}
			switch option {
			case 1:
				value, err := prefixIterator.Next()
				if value == nil && err == nil {
					fmt.Println("Iterator has reached the end of records.")
					prefixIterator.Stop()
					return
				} else if value == nil && err != nil {
					fmt.Printf("err: %v\n", err)
				} else if value != nil && err == nil {
					fmt.Printf("value: %v\n", value)
				}
			case 2:
				prefixIterator.Stop()
				return
			default:
				fmt.Println("Wrong input. Please try again.")
			}
		}
	}

}
func rangeIterate(isSStableCompressed bool, compressionMap map[string]uint64) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("Enter the minKey: ")
	scanner.Scan()
	minKey := scanner.Text()

	fmt.Print("Enter the maxKey: ")
	scanner.Scan()
	maxKey := scanner.Text()

	rangeIterator, err := iterators.NewRangeIterator(minKey, maxKey, isSStableCompressed, compressionMap)
	if rangeIterator == nil && err != nil {
		fmt.Printf("err: %v\n", err)
		return
	} else if rangeIterator != nil && err == nil {
		scanner := bufio.NewScanner(os.Stdin)
		for {
			fmt.Println("1 --> Next")
			fmt.Println("2 --> Stop")
			scanner.Scan()
			input := scanner.Text()

			option, err := strconv.Atoi(strings.TrimSpace(input))
			if err != nil {
				fmt.Println("Wrong input. Please try again.")
				continue
			}
			switch option {
			case 1:
				value, err := rangeIterator.Next()
				if value == nil && err == nil {
					fmt.Println("Iterator has reached the end of records.")
					rangeIterator.Stop()
					return
				} else if value == nil && err != nil {
					fmt.Printf("err: %v\n", err)
				} else if value != nil && err == nil {
					fmt.Printf("value: %v\n", value)
				}
			case 2:
				rangeIterator.Stop()
				return
			default:
				fmt.Println("Wrong input. Please try again.")
			}
		}
	}

}

func getRequest(engine *engine.Engine) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nChoose an option:")
		fmt.Println("1 --> Get by key")
		fmt.Println("2 --> Scanning")
		fmt.Println("3 --> Iterators")
		fmt.Println("4 --> Exit")

		scanner.Scan()
		input := scanner.Text()

		option, err := strconv.Atoi(strings.TrimSpace(input))
		if err != nil {
			fmt.Println("Wrong input. Please try again.")
			continue
		}

		switch option {
		case 1:
			getByKey(engine)
		case 2:
			scanning(engine)
		case 3:
			iterator(engine.Config.CompressionOn, engine.CompressionMap)
		case 4:
			fmt.Println("Exit.")
			return
		default:
			fmt.Println("Wrong input. Please try again.")
		}

	}

}
func scanning(engine *engine.Engine) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println("\nChoose an option:")
		fmt.Println("1 --> Prefix scan")
		fmt.Println("2 --> Range scan")
		fmt.Println("3 --> Exit")

		scanner.Scan()
		input := scanner.Text()

		option, err := strconv.Atoi(strings.TrimSpace(input))
		if err != nil {
			fmt.Println("Wrong input. Please try again.")
			continue
		}

		switch option {
		case 1:
			fmt.Print("Enter the prefix: ")
			scanner.Scan()
			prefix := scanner.Text()
			records, err := prefixScan(engine.Config.CompressionOn, prefix, engine.CompressionMap)
			if records == nil && err != nil {
				fmt.Printf("err: %v\n", err)
			} else if records != nil && err == nil {
				for _, record := range records {
					fmt.Printf("record: %v\n", record)
				}
			}
		case 2:
			records, err := rangeScan(engine.Config.CompressionOn, engine.CompressionMap)
			if records == nil && err != nil {
				fmt.Printf("err: %v\n", err)
			} else if records != nil && err == nil {
				for _, record := range records {
					fmt.Printf("record: %v\n", record)
				}
			}

		case 3:
			fmt.Println("Exit.")
			return
		default:
			fmt.Println("Wrong input. Please try again.")
		}
	}
}

func iterator(isSStableCompressed bool, compressionMap map[string]uint64) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println("\nChoose an option:")
		fmt.Println("1 --> Prefix iterate")
		fmt.Println("2 --> Range iterate")
		fmt.Println("3 --> Exit")

		scanner.Scan()
		input := scanner.Text()

		option, err := strconv.Atoi(strings.TrimSpace(input))
		if err != nil {
			fmt.Println("Wrong input. Please try again.")
			continue
		}

		switch option {
		case 1:
			prefixIterate(isSStableCompressed, compressionMap)
		case 2:
			rangeIterate(isSStableCompressed, compressionMap)
		case 3:
			fmt.Println("Exit.")
			return
		default:
			fmt.Println("Wrong input. Please try again.")
		}
	}
}
func getByKey(engine *engine.Engine) {
	var key string
	fmt.Print("Enter the key: ")
	fmt.Scanln(&key)
	if strings.HasPrefix(key, BF_KEY) || strings.HasPrefix(key, CMS_KEY) || strings.HasPrefix(key, HLL_KEY) || strings.HasPrefix(key, SH_KEY) || strings.HasPrefix(key, TB_KEY) {
		fmt.Println("key must not begin with system prefix")
		return
	}
	value, err := engine.Get(key)
	if value == nil && err != nil {
		fmt.Printf("err: %v\n", err)
	} else if value != nil && err == nil {
		fmt.Printf("value: %v\n", value)
	} else {
		fmt.Println("Record with the provided key does not exist.")
	}
}
func deleteRequest(engine *engine.Engine) {
	var key string
	fmt.Print("Enter the key: ")
	fmt.Scanln(&key)
	if strings.HasPrefix(key, BF_KEY) || strings.HasPrefix(key, CMS_KEY) || strings.HasPrefix(key, HLL_KEY) || strings.HasPrefix(key, SH_KEY) || strings.HasPrefix(key, TB_KEY) {
		fmt.Println("key must not begin with system prefix")
		return
	}
	err := engine.Delete(key)
	if err != nil {
		fmt.Printf("err: %v\n", err)
	} else {
		fmt.Println("Request Successfully Completed")
	}
}
func putRequest(engine *engine.Engine) {

	var key, value string
	fmt.Print("Enter the key: ")
	fmt.Scanln(&key)
	fmt.Print("Enter the value: ")
	fmt.Scanln(&value)
	if strings.HasPrefix(key, BF_KEY) || strings.HasPrefix(key, CMS_KEY) || strings.HasPrefix(key, HLL_KEY) || strings.HasPrefix(key, SH_KEY) || strings.HasPrefix(key, TB_KEY) {
		fmt.Println("key must not begin with system prefix")
		return
	}
	err := engine.Put(key, []byte(value))
	if err == nil {
		fmt.Println("Request Successfully Completed")
	} else {
		fmt.Printf("err: %v\n", err)
	}
}
func useBF(engine *engine.Engine) {
	records, err := prefixScan(engine.Config.CompressionOn, BF_KEY, engine.CompressionMap)
	if err != nil || len(records) == 0 {
		fmt.Println("There are no existing BloomFilters.")
	} else if records != nil && err == nil {
		for _, record := range records {
			fmt.Printf("record: %v\n", record)
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nBloomFilter")
		fmt.Println("\nChoose an option:")
		fmt.Println("1 --> Create new instance")
		fmt.Println("2 --> Delete the existing instance")
		fmt.Println("3 --> Insert new element")
		fmt.Println("4 --> Check if the element exists")
		fmt.Println("5 --> Exit")

		scanner.Scan()
		input := scanner.Text()

		option, err := strconv.Atoi(strings.TrimSpace(input))
		if err != nil {
			fmt.Println("Wrong input. Please try again.")
			continue
		}
		switch option {
		case 1:
			//create new bf
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the key: ")
			scanner.Scan()
			input := scanner.Text()
			key := BF_KEY + "_" + input

			fmt.Print("Enter the expected num of elems: ")
			scanner.Scan()
			expected := scanner.Text()
			n, err1 := strconv.Atoi(strings.TrimSpace(expected))
			if err1 != nil {
				fmt.Println("Wrong input. Please try again.")
				return
			}

			fmt.Print("Enter the false positive rate(float): ")
			scanner.Scan()
			fpr := scanner.Text()
			p, err1 := strconv.ParseFloat(fpr, 64)
			if err1 != nil {
				fmt.Println("Wrong input. Please try again.")
				return
			}

			bf := bloomFilter.NewBf(n, p)
			value := bf.Serialize()
			err := engine.Put(key, value)
			if err == nil {
				fmt.Println("Request Successfully Completed")
			} else {
				fmt.Printf("err: %v\n", err)
			}

		case 2:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the BloomFilter name: ")
			scanner.Scan()
			key := scanner.Text()
			err := engine.Delete(key)
			if err == nil {
				fmt.Println("Request Successfully Completed")
			} else {
				fmt.Printf("err: %v\n", err)
			}
		case 3:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the BloomFilter name: ")
			scanner.Scan()
			key := scanner.Text()
			value, err := engine.Get(key)
			if value == nil && err == nil {
				fmt.Println("Bloom filter does not exists.")
			} else if err != nil {
				fmt.Printf("err: %v\n", err)
			} else {
				bf := bloomFilter.Deserialize(value)
				fmt.Print("Enter the element: ")
				scanner.Scan()
				elem := scanner.Text()
				bf.Insert(elem)
				serializedBf := bf.Serialize()
				err := engine.Put(key, serializedBf)
				if err == nil {
					fmt.Println("Request Successfully Completed")
				} else {
					fmt.Printf("err: %v\n", err)
				}
			}
		case 4:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the BloomFilter name: ")
			scanner.Scan()
			key := scanner.Text()
			value, err := engine.Get(key)
			if value == nil && err == nil {
				fmt.Println("Bloom filter does not exist.")
			} else if err != nil {
				fmt.Printf("err: %v\n", err)
			} else {
				bf := bloomFilter.Deserialize(value)
				fmt.Print("Enter the element: ")
				scanner.Scan()
				elem := scanner.Text()
				exists := bf.Find(elem)
				if !exists {
					fmt.Println("Element does not exists in BloomFilter" + key)
				} else {
					fmt.Println("Element might exist in the BloomFilter: " + key)
				}
			}

		case 5:
			fmt.Println("Exit.")
			return
		default:
			fmt.Println("Wrong input. Please try again.")
		}
	}
}
func useCMS(engine *engine.Engine) {
	records, err := prefixScan(engine.Config.CompressionOn, CMS_KEY, engine.CompressionMap)
	if err != nil || len(records) == 0 {
		fmt.Println("There are no existing instances of CountMinSketch.")
	} else if records != nil && err == nil {
		for _, record := range records {
			fmt.Printf("record: %v\n", record)
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nCountMinSketch")
		fmt.Println("\nChoose an option:")
		fmt.Println("1 --> Create new instance")
		fmt.Println("2 --> Delete the existing instance")
		fmt.Println("3 --> Insert new event")
		fmt.Println("4 --> Check the frequency of events")
		fmt.Println("5 --> Exit")

		scanner.Scan()
		input := scanner.Text()

		option, err := strconv.Atoi(strings.TrimSpace(input))
		if err != nil {
			fmt.Println("Wrong input. Please try again.")
			continue
		}
		switch option {
		case 1:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the key: ")
			scanner.Scan()
			input := scanner.Text()
			key := CMS_KEY + "_" + input

			fmt.Print("Enter the epsilon(float): ")
			scanner.Scan()
			epsilon := scanner.Text()
			e, err1 := strconv.ParseFloat(epsilon, 64)
			if err1 != nil {
				fmt.Println("Wrong input. Please try again.")
				return
			}

			fmt.Print("Enter the delta(float): ")
			scanner.Scan()
			delta := scanner.Text()
			d, err1 := strconv.ParseFloat(delta, 64)
			if err1 != nil {
				fmt.Println("Wrong input. Please try again.")
				return
			}

			cms := countMinSketch.CreateCMS(e, d)
			value := cms.Serialize()

			err := engine.Put(key, value)
			if err == nil {
				fmt.Println("Request Successfully Completed")
			} else {
				fmt.Printf("err: %v\n", err)
			}

		case 2:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the CountMinSketch name: ")
			scanner.Scan()
			key := scanner.Text()
			err := engine.Delete(key)
			if err == nil {
				fmt.Println("Request Successfully Completed")
			} else {
				fmt.Printf("err: %v\n", err)
			}
		case 3:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the CountMinSketch name: ")
			scanner.Scan()
			key := scanner.Text()
			value, err := engine.Get(key)
			if value == nil && err == nil {
				fmt.Println("CountMinSketch does not exist.")
			} else if err != nil {
				fmt.Printf("err: %v\n", err)
			} else {
				cms := countMinSketch.Deserialize(value)
				fmt.Print("Enter the event: ")
				scanner.Scan()
				elem := scanner.Text()
				cms.Insert(elem)
				serializedCMS := cms.Serialize()
				err := engine.Put(key, serializedCMS)
				if err == nil {
					fmt.Println("Request Successfully Completed")
				} else {
					fmt.Printf("err: %v\n", err)
				}
			}

		case 4:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the CountMinSketch name: ")
			scanner.Scan()
			key := scanner.Text()
			value, err := engine.Get(key)
			if value == nil && err == nil {
				fmt.Println("CountMinSketch does not exists.")
			} else if err != nil {
				fmt.Printf("err: %v\n", err)
			} else {
				cms := countMinSketch.Deserialize(value)
				fmt.Print("Enter the events: ")
				scanner.Scan()
				elem := scanner.Text()
				frequency := cms.Search(elem)
				fmt.Println("There are ", frequency, " event(s)")
			}

		case 5:
			fmt.Println("Exit.")
			return
		default:
			fmt.Println("Wrong input. Please try again.")
		}
	}
}
func useHLL(engine *engine.Engine) {
	records, err := prefixScan(engine.Config.CompressionOn, HLL_KEY, engine.CompressionMap)
	if err != nil || len(records) == 0 {
		fmt.Println("There are no existing instances of HyperLogLog")
	} else if records != nil && err == nil {
		for _, record := range records {
			fmt.Printf("record: %v\n", record)
		}
	}
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nHyperLogLog")
		fmt.Println("\nChoose an option:")
		fmt.Println("1 --> Create new instance")
		fmt.Println("2 --> Delete the existing instance")
		fmt.Println("3 --> Insert new element")
		fmt.Println("4 --> Check the cardinality")
		fmt.Println("5 --> Exit")

		scanner.Scan()
		input := scanner.Text()

		option, err := strconv.Atoi(strings.TrimSpace(input))
		if err != nil {
			fmt.Println("Wrong input. Please try again.")
			continue
		}
		switch option {
		case 1:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the key: ")
			scanner.Scan()
			input := scanner.Text()
			key := HLL_KEY + "_" + input

			fmt.Print("Enter p: ")
			scanner.Scan()
			param := scanner.Text()
			p, err1 := strconv.ParseUint(param, 10, 8)
			if err1 != nil {
				fmt.Println("Wrong input. Please try again.")
				return
			}

			hll := hyperLogLog.CreateHLL(uint8(p))
			value := hll.Serialize()
			err := engine.Put(key, value)
			if err == nil {
				fmt.Println("Request Successfully Completed")
			} else {
				fmt.Printf("err: %v\n", err)
			}

		case 2:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the HyperLogLog name: ")
			scanner.Scan()
			key := scanner.Text()
			err := engine.Delete(key)
			if err == nil {
				fmt.Println("Request Successfully Completed")
			} else {
				fmt.Printf("err: %v\n", err)
			}
		case 3:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the HyperLogLog name: ")
			scanner.Scan()
			key := scanner.Text()
			value, err := engine.Get(key)
			if value == nil && err == nil {
				fmt.Println("HyperLogLog does not exists.")
			} else if err != nil {
				fmt.Printf("err: %v\n", err)
			} else {
				hll := hyperLogLog.Deserialize(value)
				fmt.Print("Enter the element: ")
				scanner.Scan()
				elem := scanner.Text()
				hll.Insert(elem)
				serializedHLL := hll.Serialize()
				err := engine.Put(key, serializedHLL)
				if err == nil {
					fmt.Println("Request Successfully Completed")
				} else {
					fmt.Printf("err: %v\n", err)
				}
			}
		case 4:
			scanner := bufio.NewScanner(os.Stdin)
			fmt.Print("Enter the HyperLogLog name: ")
			scanner.Scan()
			key := scanner.Text()
			value, err := engine.Get(key)
			if value == nil && err == nil {
				fmt.Println("HyperLogLog does not exists.")
			} else if err != nil {
				fmt.Printf("err: %v\n", err)
			} else {
				hll := hyperLogLog.Deserialize(value)
				cardinality := hll.Estimate()
				fmt.Println("There are ", cardinality, " different elements.")
			}

		case 5:
			fmt.Println("Exit.")
			return
		default:
			fmt.Println("Wrong input. Please try again.")
		}
	}
}
func useSimHash(engine *engine.Engine) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nSimHash")
		fmt.Println("\nChoose an option:")
		fmt.Println("1 --> Save fingerprint")
		fmt.Println("2 --> Calculate Hamming distance")
		fmt.Println("3 --> Exit")

		scanner.Scan()
		input := scanner.Text()

		option, err := strconv.Atoi(strings.TrimSpace(input))
		if err != nil {
			fmt.Println("Wrong input. Please try again.")
			continue
		}
		switch option {
		case 1:
			fmt.Print("Enter text: ")
			scanner.Scan()
			input := scanner.Text()

			fingerprint := simHash.GetFingerprint(input)
			byteSlice := make([]byte, 8)
			binary.BigEndian.PutUint64(byteSlice, fingerprint)
			err := engine.Put(SH_KEY+"_"+input, byteSlice)
			if err == nil {
				fmt.Println("Request Successfully Completed")
			} else {
				fmt.Printf("err: %v\n", err)
			}
		case 2:
			fmt.Print("Enter text1: ")
			scanner.Scan()
			input1 := scanner.Text()

			fmt.Print("Enter text2: ")
			scanner.Scan()
			input2 := scanner.Text()

			var fingerprint1, fingerprint2 uint64
			fp1, err1 := engine.Get(SH_KEY + "_" + input1)
			if err1 == nil && fp1 != nil {
				fingerprint1 = binary.BigEndian.Uint64(fp1)
			} else if fp1 == nil {
				fingerprint1 = simHash.GetFingerprint(input1)
			}
			fp2, err2 := engine.Get(SH_KEY + "_" + input2)
			if err2 == nil && fp2 != nil {
				fingerprint2 = binary.BigEndian.Uint64(fp2)
			} else if fp1 == nil {
				fingerprint2 = simHash.GetFingerprint(input2)
			}

			distance := simHash.HammingDistance(fingerprint1, fingerprint2)
			fmt.Println("Hamming distance: ", distance)

		case 3:
			fmt.Println("Exit.")
			return
		default:
			fmt.Println("Wrong input. Please try again.")
		}
	}
}
func useMerkle(engine *engine.Engine) {
	var err error
	fmt.Println("Please enter path of sstable:")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	path := scanner.Text()
	sstablePath := fmt.Sprintf("../data/sstable/%s", path)
	content, err := utils.GetDirContent(sstablePath)
	if err != nil {
		fmt.Println("Wrong path, please enter again.")
		return
	}
	var sstableLoaded *sstable.SSTable

	if len(content) == 1 {
		sstableLoaded, err = sstable.LoadSStableSingle(sstablePath)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		sstableLoaded, err = sstable.LoadSSTableSeparate(sstablePath)
		if err != nil {
			log.Fatal(err)
		}
	}
	sstableLoaded.LoadMerkle(len(content) != 1, sstablePath)

	// load data
	offset1 := sstableLoaded.DataOffset
	var offset2 int64
	if len(content) == 1 {
		offset2 = sstableLoaded.IndexOffset
	} else {
		offset2, err = utils.GetFileLength(sstableLoaded.Data)
		if err != nil {
			fmt.Println("Error while getting file length.")
		}
	}

	var records []*model.Record
	sstableLoaded.Data.Seek(offset1, 0)
	for offset1 < offset2 {
		record, bytesRead, err := model.Deserialize(sstableLoaded.Data, engine.Config.CompressionOn, engine.CompressionMap)
		if err != nil {
			fmt.Println("Error while deserializing record.", err)
			return
		}
		records = append(records, record)
		offset1 += int64(bytesRead)
	}

	var bytesToCheck [][]byte
	for _, record := range records {
		bytesToAppend, _ := record.Serialize(engine.Config.CompressionOn, engine.CompressionMap)
		bytesToCheck = append(bytesToCheck, bytesToAppend)
	}

	response, _ := sstableLoaded.Merkle.VerifyTree(bytesToCheck)

	fmt.Println("Changes are on these records: ")

	for _, change := range response {
		fmt.Println(change)
	}

}
func probStructs(engine *engine.Engine) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Println("\nChoose an option:")
		fmt.Println("1 --> BloomFilter")
		fmt.Println("2 --> CountMinSketch")
		fmt.Println("3 --> HyperLogLog")
		fmt.Println("4 --> SimHash")
		fmt.Println("5 --> Exit")

		scanner.Scan()
		input := scanner.Text()

		structure, err := strconv.Atoi(strings.TrimSpace(input))
		if err != nil {
			fmt.Println("Wrong input. Please try again.")
			continue
		}

		switch structure {
		case 1:
			useBF(engine)
		case 2:
			useCMS(engine)
		case 3:
			useHLL(engine)
		case 4:
			useSimHash(engine)
		case 5:
			fmt.Println("Exit.")
			return
		default:
			fmt.Println("Wrong input. Please try again.")
		}

	}
}
func StartEngine() {
	engine, err := engine.NewEngine()
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Println("\nChoose an option:")
		fmt.Println("1 --> Put")
		fmt.Println("2 --> Get")
		fmt.Println("3 --> Delete")
		fmt.Println("4 --> Use probabilistic structures")
		fmt.Println("5 --> Use Merkle Tree")
		fmt.Println("6 --> Clear Log")
		fmt.Println("7 --> Exit")

		scanner.Scan()
		input := scanner.Text()

		choice, err := strconv.Atoi(strings.TrimSpace(input))
		if err != nil {
			fmt.Println("Wrong input. Please try again.")
			continue
		}

		switch choice {
		case 1:
			//Put
			putRequest(engine)
		case 2:
			//Get
			getRequest(engine)
		case 3:
			//Delete
			deleteRequest(engine)
		case 4:
			//Use probabilistic structures
			probStructs(engine)
		case 5:
			useMerkle(engine)
		case 6:
			err := engine.Wal.ClearLog()
			if err != nil {
				log.Fatal(err)
			}
		case 7:
			fmt.Println("Exit program.")
			engine.Exit()
			return
		default:
			fmt.Println("Wrong input. Please try again.")
			continue
		}
	}
}
