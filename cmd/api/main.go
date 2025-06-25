package main

import (
	"context"
	"github.com/upekZ/matching-engine/internal/api/grpc"
	"github.com/upekZ/matching-engine/internal/api/rest"
	"github.com/upekZ/matching-engine/internal/engine"
	"github.com/upekZ/matching-engine/internal/handlers"
	redisBroker "github.com/upekZ/matching-engine/internal/message-broker"
	storage "github.com/upekZ/matching-engine/internal/storage/db-writer"
	redisCache "github.com/upekZ/matching-engine/internal/storage/redis-store"
	sqlc2 "github.com/upekZ/matching-engine/internal/storage/sqlc"
	"log"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisCacheClient, cacheErr := redisCache.New()
	if cacheErr != nil {
		log.Printf("Error creating redisCache cache: %v", cacheErr)
	}

	redisMsgBroker, brokerError := redisBroker.New()
	if brokerError != nil {
		log.Fatalf("Error creating redis broker: %v", brokerError)
	}

	handler := handlers.NewHandlerFactory(redisCacheClient, redisMsgBroker)

	eng := engine.New(handler)

	err := grpc.New("8080", redisMsgBroker)
	if err != nil {
		log.Fatalf("Error starting grpc server: %v", brokerError)
	}

	server := rest.New(eng)

	//db-engine can run in isolation with a cache reader
	if dbErr := storage.New(ctx, sqlc2.CreateDBHandler(), redisCacheClient); dbErr != nil {
		log.Printf("DB Start Failure: %v", dbErr)
	}

	if err := server.Start(); err != nil {
		log.Fatalf("Error starting Rest server: %v", err)
	}

	// OS signals should be handled. Wait for the context and flush logs before terminating
}
