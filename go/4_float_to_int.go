package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Stats2 struct {
	Min     int64
	Max     int64
	Average float64
	Count   int
}

// Tune these for your machine
const (
	batchSize = 100000 // lines per batch
	inputFile = "../data/measurements_1b.txt"
)

func multiple_optimizations(verbose bool) {
	start := time.Now()

	workerCount := 32 * runtime.NumCPU()

	// Channel for batched lines
	batches := make(chan []string, workerCount)

	// Worker goroutine setup
	var wg sync.WaitGroup
	workerMaps := make([]map[string][]int64, workerCount)
	for w := 0; w < workerCount; w++ {
		workerMaps[w] = make(map[string][]int64)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			localMap := workerMaps[idx]
			for batch := range batches {
				for _, line := range batch {
					elements := strings.Split(line, ";")
					if len(elements) != 2 {
						continue
					}
					city := elements[0]
					tempFloat, err := strconv.ParseFloat(elements[1], 64)
					if err != nil {
						continue
					}
					tempInt := int64(math.Round(tempFloat * 10))
					localMap[city] = append(localMap[city], tempInt)
				}
			}
		}(w)
	}

	// Producer: read and send batches
	go func() {
		file, err := os.Open(inputFile)
		if err != nil {
			panic(err)
		}
		defer file.Close()
		scanner := bufio.NewScanner(file)
		batch := make([]string, 0, batchSize)
		for scanner.Scan() {
			batch = append(batch, scanner.Text())
			if len(batch) == batchSize {
				batchCopy := make([]string, batchSize)
				copy(batchCopy, batch)
				batches <- batchCopy
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			batchCopy := make([]string, len(batch))
			copy(batchCopy, batch)
			batches <- batchCopy
		}
		close(batches)
	}()

	wg.Wait()

	// Merge all worker maps into global map
	merged := make(map[string][]int64)
	for _, m := range workerMaps {
		for city, temps := range m {
			merged[city] = append(merged[city], temps...)
		}
	}

	numReducers := runtime.NumCPU()
	cityKeys := make([]string, 0, len(merged))
	for city := range merged {
		cityKeys = append(cityKeys, city)
	}
	chunkSize := (len(cityKeys) + numReducers - 1) / numReducers

	var wg1 sync.WaitGroup
	wg1.Add(numReducers)
	resultsChan := make(chan string, len(cityKeys))

	for i := 0; i < numReducers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(cityKeys) {
			end = len(cityKeys)
		}
		go func(keys []string) {
			defer wg1.Done()
			for _, city := range keys {
				temps := merged[city]
				min, max := temps[0], temps[0]
				var sum int64
				for _, temp := range temps {
					if temp < min {
						min = temp
					}
					if temp > max {
						max = temp
					}
					sum += temp
				}
				avg := float64(sum) / float64(len(temps))
				avgRounded := math.Round(avg) / 10.0
				minRounded := float64(min) / 10.0
				maxRounded := float64(max) / 10.0
				count := len(temps)
				// send result string to resultsChan for bundled output
				resultsChan <- fmt.Sprintf(
					"City: %s, Min: %.1f, Max: %.1f, Average: %.1f, Count: %d",
					city, minRounded, maxRounded, avgRounded, count,
				)
			}
		}(cityKeys[start:end])
	}

	wg1.Wait()
	close(resultsChan)

	fmt.Printf("\nCities: %d, Execution time: %s\n", len(resultsChan), time.Since(start))
}
