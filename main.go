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

		// 打印每个 GPU 的使用情况
		for i := 0; i < gpuCount; i++ {
			fmt.Printf("GPU %d - Memory Usage: %.2f%%, Utilization: %.2f%%\n", i, memoryUsage[i], utilization[i])
		}

		// 计算总内存使用率和总 GPU 利用率
		totalMemoryUsage := 0.0
		totalUtilization := 0.0
		for _, mem := range memoryUsage {
			totalMemoryUsage += mem
		}
		for _, util := range utilization {
			totalUtilization += util
		}
		averageMemoryUsage := totalMemoryUsage / float64(gpuCount)
		averageUtilization := totalUtilization / float64(gpuCount)

		// 根据总占用率判断是否延迟或停止消费
		if averageMemoryUsage >= 70.0 || averageUtilization >= 70.0 {
			fmt.Println("资源使用率过高，停止消费...")
			time.Sleep(5000 * time.Second) // 暂停 5 秒
			continue
		} else if averageMemoryUsage >= 50.0 || averageUtilization >= 50.0 {
			fmt.Println("资源使用率达到 50%，延迟消费...")
			time.Sleep(2000 * time.Second) // 延迟消费 2 秒
		}

		// 模拟创建容器来执行模型
		fmt.Println("Creating container to execute model...")
		// 这里可以添加你的容器创建逻辑
		time.Sleep(2000 * time.Second) // 模拟创建容器所需时间
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)

	go Consumer(&wg)

	// 等待消费者 goroutine 完成
	wg.Wait()
}