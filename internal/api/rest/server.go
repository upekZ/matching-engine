package rest

import (
	"fmt"
	"log"
	"net/http"
	"time"
)

type Server struct {
	service MatchingEngine
}

func NewServer(service MatchingEngine) *Server {
	return &Server{
		service: service,
	}
}

func (app *Server) Start() error {

	server := &http.Server{
		Addr:         ":3000",
		Handler:      app.loadChiRoutes(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Println("Starting REST server...")

	err := server.ListenAndServe()
	if err != nil {
		return fmt.Errorf("server start failure: %w", err)
	}

	return err
}
