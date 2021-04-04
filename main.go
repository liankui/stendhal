package main

import (
	"github.com/asim/go-micro/v3"
	"github.com/liankui/stendhal/examples/kubernetes/go/micro"
)

func main() {

	// create a new service
	service := micro.NewService(
		micro.Name("helloworld"),
	)

	// initialise flags
	service.Init()

	// start the service
	service.Run()
}
