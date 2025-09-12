package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

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
