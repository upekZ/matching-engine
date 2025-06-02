package rest

import (
	"fmt"
	"net/http"
	"time"
)

type Server struct {
	service OrderService
}

func NewServer(service OrderService) *Server {
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

	err := server.ListenAndServe()
	if err != nil {
		return fmt.Errorf("server start failure: %w", err)
	}

	return err
}
