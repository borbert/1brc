package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

func final_attempt(verbose bool) {
	start := time.Now()

	// extract the data from the file
	// cityTemps := extract_data("../data/measurements_1b.txt")\
	filename := "../data/measurements_1b.txt"
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	fmt.Println("File opened successfully")

	cityTemps := make(map[string]*Stats)

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		elements := strings.Split(line, ";")
		city := elements[0]
		temp, _ := strconv.ParseFloat(elements[1], 64)

		stat, ok := cityTemps[city]
		if !ok {
			cityTemps[city] = &Stats{
				Min:     temp,
				Max:     temp,
				Average: temp,
				Count:   1,
			}
		} else {
			stat.Count++
			if temp < stat.Min {
				stat.Min = temp
			}
			if temp > stat.Max {
				stat.Max = temp
			}
			// Online average (mean) update
			stat.Average += (temp - stat.Average) / float64(stat.Count)
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println("Error scanning file:", err)
	}
	fmt.Printf("\nExecution time: %s\n", time.Since(start))

}
