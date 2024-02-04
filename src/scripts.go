package main

import (
	"fmt"
	"math/rand"

	"github.com/natasakasikovic/Key-Value-engine/src/system"
)

// Puts 100000 records with a total of 100 different keys into the system
func Put100() {
	engine, err := system.NewEngine()

	if err != nil {
		return
	}

	var possibleKeys []string = make([]string, 0)

	for i := 0; i < 100; i++ {
		possibleKeys = append(possibleKeys, fmt.Sprint(i))
	}

	//Shuffle the keys around
	rand.Shuffle(len(possibleKeys), func(i, j int) { possibleKeys[i], possibleKeys[j] = possibleKeys[j], possibleKeys[i] })

	for i := 0; i < 100000; i++ {
		// if i%1000 == 0 {
		// 	fmt.Printf("Record %d has been put\n", i)
		// }
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

// Puts 100000 records with a total of 50000 different keys into the system
func Put50000() {
	engine, err := system.NewEngine()

	if err != nil {
		return
	}

	//Array containing all possible keys
	var possibleKeys []string = make([]string, 0)

	//Generate the keys
	for i := 0; i < 50000; i++ {
		possibleKeys = append(possibleKeys, fmt.Sprint(i))
	}

	//Shuffle the keys around
	rand.Shuffle(len(possibleKeys), func(i, j int) { possibleKeys[i], possibleKeys[j] = possibleKeys[j], possibleKeys[i] })

	for i := 0; i < 100000; i++ {
		if i%1000 == 0 {
			fmt.Printf("Record %d has been put\n", i)
		}
		err = engine.Put(possibleKeys[i%50000], []byte(possibleKeys[i%50000]))
		if err != nil {
			fmt.Printf("Error during put %d: %s\n", i, err.Error())
			return
		}
	}

	test := 0
	test += 1
	engine.Exit()
}
