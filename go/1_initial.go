package main

import (
	"fmt"
	"time"
)

func Initial(verbose bool) {
	start := time.Now()

	// extract the data from the file
	cityTemps := extract_data("../data/measurements_1b.txt")

	iter := 0
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
