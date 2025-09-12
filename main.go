package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Stats struct {
	Min     float64
	Max     float64
	Average float64
	Sum     float64
	Count   int
}

func main() {
	start := time.Now()

	file, err := os.Open("data/measurements_1b.txt")
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	fmt.Println("File opened successfully")

	cityTemps := make(map[string][]float64)

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

	for city, temps := range cityTemps {
		stats := Stats{Min: temps[0], Max: temps[0], Average: 0, Sum: 0, Count: 0}
		for _, temp := range temps {
			if temp < stats.Min {
				stats.Min = temp
			}
			if temp > stats.Max {
				stats.Max = temp
			}
			stats.Count++
			stats.Sum += temp
			stats.Average = stats.Sum / float64(stats.Count)
		}

		fmt.Printf(
			"City: %s, Min: %.2f, Max: %.2f, Average: %.2f, Count: %d\n",
			city, stats.Min, stats.Max, stats.Average, stats.Count,
		)
	}

	fmt.Printf("Execution time: %s\n", time.Since(start))
}
