package main

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"sync"
	"time"
	"os/exec"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Consumer struct
type Consumer struct {
	ID         int
	Consumer   *kafka.Consumer
	quitChan   chan bool
	wg         *sync.WaitGroup
	paused     bool
	frequency  time.Duration // 拉取消息的间隔
}

func NewConsumer(consumerID int, wg *sync.WaitGroup) *Consumer {
	// 配置消费者
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",  // Kafka broker 地址
		"group.id":          "test_group",      // 消费者组
		"auto.offset.reset": "earliest",        // 从最早的消息开始消费
	}

	// 创建消费者实例
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("消费者 %d 创建失败: %v\n", consumerID, err)
	}

	// 订阅主题
	topic := "test_topic"
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatalf("消费者 %d 订阅主题失败: %v\n", consumerID, err)
	}

	return &Consumer{
		ID:        consumerID,
		Consumer:  consumer,
		quitChan:  make(chan bool),
		wg:        wg,
		paused:    false,
		frequency: 100 * time.Millisecond, // 默认频率
	}
}

func (c *Consumer) Start() {
	defer c.wg.Done()

	// 开始消费消息
	for {
		select {
		case <-c.quitChan:
			fmt.Printf("消费者 %d 停止\n", c.ID)
			c.Consumer.Close()
			return
		default:
			if c.paused {
				// 如果消费者被暂停，则跳过消息处理
				continue
			}

			// 正常消费
			msg, err := c.Consumer.ReadMessage(c.frequency)
			if err != nil {
				log.Printf("消费者 %d 读取消息失败: %v\n", c.ID, err)
				continue
			}

			// 打印消息内容
			fmt.Printf("消费者 %d 接收到消息: %s\n", c.ID, string(msg.Value))
		}
	}
}

// Pause 停止消费消息
func (c *Consumer) Pause() {
	if !c.paused {
		fmt.Printf("消费者 %d 暂停消费\n", c.ID)
		partitions, _ := c.Consumer.Assignment()
		c.Consumer.Pause(partitions) // 暂停所有分区
		c.paused = true
	}
}

// Resume 恢复消费消息
func (c *Consumer) Resume() {
	if c.paused {
		fmt.Printf("消费者 %d 恢复消费\n", c.ID)
		partitions, _ := c.Consumer.Assignment()
		c.Consumer.Resume(partitions) // 恢复所有分区
		c.paused = false
	}
}

func (c *Consumer) Stop() {
	c.quitChan <- true
}

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

func getResourceUsage() float64 {
	// 获取 GPU 使用率的平均值
	_, _, utilization, err := MonitorGPU()
	if err != nil {
		log.Printf("无法获取 GPU 使用率: %v\n", err)
		return 100
	}

	// 计算所有 GPU 使用率的平均值
	var totalUtil float64
	for _, util := range utilization {
		totalUtil += util
	}
	return totalUtil / float64(len(utilization))
}

func main() {
	var wg sync.WaitGroup
	consumers := make(map[int]*Consumer)

	// 捕获系统中断（Ctrl+C）信号，以便优雅退出
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	// 假设消费者数量由配置文件/变量确定，这里硬编码为 3
	numConsumers := 3

	// 启动 3 个消费者
	for i := 1; i <= numConsumers; i++ {
		wg.Add(1)
		newConsumer := NewConsumer(i, &wg)
		consumers[i] = newConsumer
		go newConsumer.Start()
	}

	// 资源监控：根据系统资源使用情况动态调整消费行为
	go func() {
		for {
			usage := getResourceUsage()
			fmt.Printf("当前GPU使用率: %.2f%%\n", usage)

			if usage >= 70 {
				// 如果资源使用率超过 70%，则暂停消费
				for _, consumer := range consumers {
					consumer.Pause()
				}
				fmt.Println("资源使用率达到 70%，暂停消费")
			} else if usage >= 50 {
				// 如果资源使用率超过 50%，则延迟消费
				// 延迟 1 分钟，即每次拉取间隔为 1 分钟
				for _, consumer := range consumers {
					consumer.frequency = 1 * time.Minute
				}
				fmt.Println("资源使用率达到 50%，延迟消费 1 分钟")
			} else {
				// 资源使用率低于 50%，恢复正常消费
				for _, consumer := range consumers {
					consumer.frequency = 100 * time.Millisecond // 恢复正常拉取频率
					consumer.Resume()
				}
			}

			time.Sleep(5 * time.Second) // 每 5 秒检查一次资源使用率
		}
	}()

	// 等待系统信号以优雅退出
	<-signalChan

	// 等待所有消费者退出
	wg.Wait()
}
