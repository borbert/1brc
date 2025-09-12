package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

func Initial(verbose bool) {
	start := time.Now()

	// extract the data from the file
	cityTemps := extract_data("../data/measurements_1b.txt")

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

func extract_data(filename string) map[string][]float64 {
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	fmt.Println("File opened successfully")

	cityTemps := make(map[string][]float64)

	// maybe add some indicator for progress???
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		elements := strings.Split(line, ";")
		city := elements[0]
		temp, _ := strconv.ParseFloat(elements[1], 64)
		cityTemps[city] = append(cityTemps[city], temp)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error scanning file:", err)
	}

	return cityTemps
}
