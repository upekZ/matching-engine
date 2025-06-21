package rest

import (
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"net/http"
	"time"
)

type MatchingEngine interface {
	OnNewRequest(order *models.Order) models.Order
}

type Server struct {
	matcher MatchingEngine
}

func NewServer(me MatchingEngine) *Server {
	return &Server{
		matcher: me,
	}
}

func (s *Server) Start() error {

	server := &http.Server{
		Addr:         ":3000",
		Handler:      s.loadChiRoutes(),
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

func (s *Server) newOrderRequest(writer http.ResponseWriter, req *http.Request) {
	var order models.Order

	if err := json.NewDecoder(req.Body).Decode(&order); err != nil {
		log.Printf("error decoding body: %v", err)
		http.Error(writer, "order request failure", http.StatusInternalServerError)
		return
	}
	orderResp := s.matcher.OnNewRequest(&order)

	if err := writeJSON(writer, http.StatusCreated, orderResp); err != nil {
		log.Printf("Error writing response: %v", err)
		http.Error(writer, "order request failure", http.StatusInternalServerError)
	}
}

func (s *Server) loadChiRoutes() *chi.Mux {
	router := chi.NewRouter()

	router.Use(middleware.Logger)

	router.Get("/", func(writer http.ResponseWriter, reader *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})

	router.Route("/orders", s.loadUserRoutes)

	return router
}

func (s *Server) loadUserRoutes(router chi.Router) {

	router.Post("/", s.newOrderRequest)
}

func writeJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(status)

	return json.NewEncoder(w).Encode(v)
}
