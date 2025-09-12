package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func third_concurrency(verbose bool) {
	start := time.Now()

	lines := make(chan []string, 100)             // buffered channel
	resultChan := make(chan map[string][]float64) // channel for results

	var wg sync.WaitGroup
	wg.Add(1)

	// reader
	go func() {
		file, err := os.Open("../data/measurements_1b.txt")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		batchSize := 100
		batch := make([]string, 0, batchSize)

		for scanner.Scan() {
			if len(batch) == batchSize {
				batchCopy := make([]string, batchSize)
				copy(batchCopy, batch)
				lines <- batchCopy
				batch = batch[:0]
			}
			if len(batch) > 0 {
				batchCopy := make([]string, len(batch))
				copy(batchCopy, batch)
				lines <- batchCopy

			}

		}
		fmt.Println("File scanned successfully")
		close(lines)
	}()

	// Processor
	go func() {
		defer wg.Done()
		cityTemps := make(map[string][]float64)
		for batch := range lines {
			for _, line := range batch {
				elements := strings.Split(line, ";")
				city := elements[0]
				temp, _ := strconv.ParseFloat(elements[1], 64)
				cityTemps[city] = append(cityTemps[city], temp)
			}
		}
		resultChan <- cityTemps
		fmt.Println("Data processed into map successfully")
	}()

	cityTemps := <-resultChan
	wg.Wait()

	// Process the map
	iter := 0
	var avg float64

	for city, temps := range cityTemps {
		stats := Stats{Min: temps[0], Max: temps[0], Average: 0, Count: 0}
		for _, temp := range temps {
			if temp < stats.Min {
				stats.Min = temp
			}
			if temp > stats.Max {
				stats.Max = temp
			}
			stats.Count++
			avg += temp
		}

		avg /= float64(stats.Count)
		stats.Average = math.Ceil(avg*10) / 10

		if verbose {
			fmt.Printf(
				"City: %s, Min: %.2f, Max: %.2f, Average: %.2f, Count: %d\n",
				city, stats.Min, stats.Max, stats.Average, stats.Count,
			)
		}

		iter++
		fmt.Printf("\rProcessing Cities:  %d", iter)
	}

	fmt.Printf("\nExecution time: %s\n", time.Since(start))
}
