package main

import (
	"github.com/upekZ/matching-engine/internal/api/grpc"
	"github.com/upekZ/matching-engine/internal/api/rest"
	"github.com/upekZ/matching-engine/internal/engine"
	redis "github.com/upekZ/matching-engine/internal/storage/redis-store"
)

func main() {
	redisClient, err := redis.NewClient("localhost:6379")
	if err != nil {
		panic(err)
	}

	eng := engine.New(redisClient)

	_, err = grpc.NewServer("8080", eng)
	if err != nil {
		panic(err)
	}

	server := rest.NewServer(eng)

	if err := server.Start(); err != nil {
		panic(err)
	}
}
