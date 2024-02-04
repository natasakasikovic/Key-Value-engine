package main

import (
	"fmt"
	"math/rand"

	"github.com/natasakasikovic/Key-Value-engine/src/structs/scan"
	"github.com/natasakasikovic/Key-Value-engine/src/system"
)

func Put100() {
	engine, err := system.NewEngine()

	if err != nil {
		return
	}

	var possibleKeys []string = make([]string, 0)

	for i := 0; i < 100; i++ {
		possibleKeys = append(possibleKeys, fmt.Sprint(i))
	}

	rand.Shuffle(len(possibleKeys), func(i, j int) { possibleKeys[i], possibleKeys[j] = possibleKeys[j], possibleKeys[i] })

	for i := 0; i < 100000; i++ {
		if i%1000 == 0 {
			fmt.Printf("Record %d has been put\n", i)
		}
		err = engine.Put(possibleKeys[i%100], []byte(possibleKeys[i%100]))
		if err != nil {
			fmt.Printf("Error during put %d: %s\n", i, err.Error())
			return
		}
	}

	test := 0
	test += 1
	engine.Exit()
}

func pretrazi() {
	engine, err := system.NewEngine()
	if err != nil {
		fmt.Printf("Error loading engine: %s\n", err)
		return
	}
	for i := 0; i < 200; i++ {
		value, err := engine.Get(fmt.Sprint(i))
		if err != nil {
			fmt.Printf("Error getting key %d: %s\n", i, err.Error())
			engine.Exit()
			return
		}
		if value != nil {
			fmt.Printf("Key: %d, Value: %s\n", i, string(value))
		}

	}
	engine.Exit()
}

func probajSken() {
	engine, err := system.NewEngine()
	if err != nil {
		fmt.Printf("Error loading engine: %s\n", err.Error())
		return
	}
	defer engine.Exit()

	prefix := "1"
	fmt.Printf("Prefix '%s' scan, page 1\n", prefix)
	records, err := scan.PrefixScan("1", 1, 15, engine.Config.CompressionOn, engine.CompressionMap)
	if err != nil {
		fmt.Printf("Greska pri prefix scan: %s\n", err.Error())
	}

	for i := 0; i < len(records); i++ {
		fmt.Printf("%d. Rekord: %s\n", i+1, records[i].String())
	}

	rangeMin, rangeMax := "1", "9"
	fmt.Printf("Range scan from '%s' to '%s', page 1\n", rangeMin, rangeMax)
	records, err = scan.RangeScan(rangeMin, rangeMax, 1, 15, engine.Config.CompressionOn, engine.CompressionMap)
	if err != nil {
		fmt.Printf("Greska pri prefix scan: %s\n", err.Error())
	}

	for i := 0; i < len(records); i++ {
		fmt.Printf("%d. Rekord: %s\n", i+1, records[i].String())
	}
}

func main() {
	// Put100()
	pretrazi()
	probajSken()
}
