package main

import (
	"bytes"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"time"
)

/*
消息预取
设置预取数量 Prefetch Count 限制消费者在收到下一确认回执前执行一次醉倒可以就少多少条消息。
如：Prefetch Count=1 表示RabbitMQ服务器每次给每个消费者发送一条消息，在收到该消费Ack指令之前RabbitMQ不会再向该消费者发送新的消息。
*/

func main() {
	// 建立连接
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// 获取channel
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 声明队列
	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	err = ch.Qos(
		1,     // prefetch count	这里设置为1。这告诉RabbitMQ不要一次向一个worker发出多个消息。
		// 或者，换句话说，在处理并确认前一条消息之前，不要向worker发送新消息。相反，它将把它发送给下一个不忙的worker。
		0,     // prefetch size
		false, // global
	)
	if err != nil {
		fmt.Printf("ch.Qos() failed, err:%v\n", err)
		return
	}

	// 获取接收消息的Delivery通道
	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack 注意这里如果传false,关闭自动消息确认
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			dot_count := bytes.Count(d.Body, []byte(".")) // 数一下有几个.
			t := time.Duration(dot_count)
			time.Sleep(t * time.Second) // 模拟耗时的任务
			log.Printf("Done")
			d.Ack(false) // 手动传递消息确认
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
