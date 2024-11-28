package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"sync"
	"time"
)

// MonitorGPU retrieves the number of GPUs, their memory usage, and utilization using nvidia-smi
func MonitorGPU() (int, []float64, []float64, error) {
	cmd := exec.Command("nvidia-smi", "--query-gpu=index,memory.used,memory.total,utilization.gpu", "--format=csv,noheader,nounits")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return 0, nil, nil, fmt.Errorf("failed to execute nvidia-smi: %v", err)
	}

	lines := bytes.Split(out.Bytes(), []byte("\n"))
	gpuCount := len(lines) - 1 // Last line is empty
	memoryUsage := make([]float64, gpuCount)
	utilization := make([]float64, gpuCount)

	for i, line := range lines {
		if len(line) == 0 {
			continue
		}
		var index, used, total, util int
		// Adjust the format string to match the output
		_, err := fmt.Sscanf(string(line), "%d, %d, %d, %d", &index, &used, &total, &util)
		if err != nil {
			return 0, nil, nil, fmt.Errorf("failed to parse line: %s, error: %v", line, err)
		}
		memoryUsage[i] = float64(used) / float64(total) * 100 // Memory usage percentage
		utilization[i] = float64(util)                        // GPU utilization percentage
	}

	return gpuCount, memoryUsage, utilization, nil
}

// Consumer simulates the consumer that creates containers
func Consumer(wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		gpuCount, memoryUsage, utilization, err := MonitorGPU()
		if err != nil {
			fmt.Println("Error monitoring GPU resources:", err)
			return
		}

		for i := 0; i < gpuCount; i++ {
			fmt.Printf("GPU %d - Memory Usage: %.2f%%, Utilization: %.2f%%\n", i, memoryUsage[i], utilization[i])
		}

		// Check if any GPU exceeds the thresholds
		highUsage := false
		for _, mem := range memoryUsage {
			if mem > 80.0 { // Set your memory threshold
				highUsage = true
				break
			}
		}
		for _, util := range utilization {
			if util > 80.0 { // Set your utilization threshold
				highUsage = true
				break
			}
		}

		if highUsage {
			fmt.Println("GPU resources are high, waiting...")
			time.Sleep(5 * time.Second) // Block and wait before checking again
			continue
		}

		// Simulate creating a container to execute the model
		fmt.Println("Creating container to execute model...")
		// Here you would add your container creation logic
		time.Sleep(2 * time.Second) // Simulate time taken to create a container
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	go Consumer(&wg)

	// Wait for the consumer goroutine to finish
	wg.Wait()
}
