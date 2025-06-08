package main

import (
	"context"
	"github.com/upekZ/matching-engine/internal/api/grpc"
	"github.com/upekZ/matching-engine/internal/api/rest"
	"github.com/upekZ/matching-engine/internal/engine"
	redisBroker "github.com/upekZ/matching-engine/internal/message-broker"
	storage "github.com/upekZ/matching-engine/internal/storage/db-writer"
	redisCache "github.com/upekZ/matching-engine/internal/storage/redis-store"

	"log"
)

func main() {

	redisCacheClient, cacheErr := redisCache.NewCacheClient("localhost:6379")
	if cacheErr != nil {
		log.Printf("Error creating redisCache cache: %v", cacheErr)
	}

	redisMsgBroker, brokerError := redisBroker.NewMessageBroker("localhost:6379")
	if brokerError != nil {
		log.Fatalf("Error creating redisCache broker: %v", brokerError)
	}

	eng := engine.New(redisMsgBroker, redisCacheClient)

	_, err := grpc.NewServer("8080", eng)
	if err != nil {
		panic(err)
	}

	server := rest.NewServer(eng)

	//dbengine can run in isolation with a cache reader
	if dbErr := storage.RunDBEngine(context.Background(), redisCacheClient, 1000000, 1000); dbErr != nil {
		log.Printf("DB Start Failure: %v", dbErr)
	}

	if err := server.Start(); err != nil {
		panic(err)
	}
}
