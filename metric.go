package main

import (
	"regexp"
	"os"
	"bufio"
)

type Metric struct {
	name string
	amount int64
	value float64
	timestamp int64
}
/*
	This is a "theoretical" size of 1 metric
	Maximum size of retry file then we can calculate as
	MaxMetrics*ClientSendInterval*metricsSize*metricsSize
	Which will give you cache for 1 very bad minute
 */

// Check metric to match base metric regexp
func validateMetric(metric string, reg string) bool {
	match, _ := regexp.MatchString(reg, metric)
	return match
}

// Reading metrics from file and remove file afterwords
func readMetricsFromFile(file string) []string {
	defer os.Remove(file)

	var results_list []string
	f, err := os.Open(file)

	if err != nil {
		return results_list
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		results_list = append(results_list, scanner.Text())
	}

	return results_list
}

// Reading metrics from file and remove file afterwords
func getSizeInLinesFromFile(file string) int {
	res := 0

	f, err := os.Open(file)
	if err != nil {
		return res
	}

	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		res++
	}

	return res
}