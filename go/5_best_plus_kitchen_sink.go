package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

func kitchenSink(verbose bool) {
	start := time.Now()
	workerCount := 32 * runtime.NumCPU()
	batches := make(chan []string, workerCount)
	var wg sync.WaitGroup
	workerMaps := make([]map[string]*Stats, workerCount)

	for w := 0; w < workerCount; w++ {
		workerMaps[w] = make(map[string]*Stats)
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
					temp, err := strconv.ParseFloat(elements[1], 64)
					if err != nil {
						continue
					}
					s, exists := localMap[city]
					if !exists {
						localMap[city] = &Stats{Min: temp, Max: temp, Average: temp, Count: 1}
					} else {
						s.Count++
						if temp < s.Min {
							s.Min = temp
						}
						if temp > s.Max {
							s.Max = temp
						}
						// Online average update (Wselford)
						s.Average += (temp - s.Average) / float64(s.Count)
					}
				}
			}
		}(w)
	}

	// Producer goroutine: batch and send lines
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

	wg.Wait()

	// --- Merge all worker maps into final city stats ---
	finalStats := make(map[string]*Stats)

	for _, localMap := range workerMaps {
		for city, s := range localMap {
			fs, exists := finalStats[city]
			if !exists {
				finalStats[city] = &Stats{
					Min:     s.Min,
					Max:     s.Max,
					Average: s.Average,
					Count:   s.Count,
				}
			} else {
				// Merge: update min/max, combine means and counts
				// Online mean update for two groups:
				// https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
				totalCount := fs.Count + s.Count
				if s.Min < fs.Min {
					fs.Min = s.Min
				}
				if s.Max > fs.Max {
					fs.Max = s.Max
				}
				// Update mean: weighted
				fs.Average = (fs.Average*float64(fs.Count) + s.Average*float64(s.Count)) / float64(totalCount)
				fs.Count = totalCount
			}
		}
	}
	fmt.Printf("\nDone. Cities: %d, Execution time: %s\n", len(finalStats), time.Since(start))
}
