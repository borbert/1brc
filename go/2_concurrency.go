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

func second_concurrency(verbose bool) {
	start := time.Now()

	lines := make(chan string, 100)               // buffered channel
	resultChan := make(chan map[string][]float64) // channel for results

	var wg sync.WaitGroup
	wg.Add(1)

	// reader
	go func() {
		file, _ := os.Open("../data/measurements_1b.txt")
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lines <- scanner.Text()
		}
		fmt.Println("File scanned successfully")
		close(lines)
	}()

	// Processor
	go func() {
		defer wg.Done()
		cityTemps := make(map[string][]float64)
		for line := range lines {
			elements := strings.Split(line, ";")
			city := elements[0]
			temp, _ := strconv.ParseFloat(elements[1], 64)
			cityTemps[city] = append(cityTemps[city], temp)
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
