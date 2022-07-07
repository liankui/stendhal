package main

import (
	"github.com/streadway/amqp"
	"log"
	"os"
	"strings"
)

/*
数据持久化，包含3类持久化
Queue持久化，durable=true
Message持久化，deliveryMode=2 （只有Queue持久化，服务器重启Queue后，没发出去的消息会丢失）
Exchange持久化，durable=true todo 这里没看到在哪里设置了？
出现的场景问题：
如果消费者收到消息时autoAck为true，但消费端还没处理完就宕机，在这种情况下消息数据还是丢失了
在这种场景下需要把atupAck设为false，并在消费逻辑完成之后再手动去确认。（如果没有确认，Broker会把消息发送给其他消费者）

*/


func main() {
	// 1. 尝试连接RabbitMQ，建立连接
	// 该连接抽象了套接字连接，并为我们处理协议版本协商和认证等。
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	// 2. 接下来，我们创建一个通道，大多数API都是用过该通道操作的。
	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	// 3. 声明消息要发送到的队列
	q, err := ch.QueueDeclare(
		"task_queue", // name
		true,   // durable 声明为持久队列
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	body := bodyFrom(os.Args)  // 从参数中获取要发送的消息正文
	// 4.将消息发布到声明的队列
	err = ch.Publish(
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // 持久（交付模式：瞬态/持久）
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	failOnError(err, "Failed to publish a message")
}

func bodyFrom(args []string) string {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "hello"
	} else {
		s = strings.Join(args[1:], " ")
	}
	return s
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
