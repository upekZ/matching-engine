package rest

import (
	"encoding/json"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"net/http"
)

type OrderService interface {
	PlaceRequest(order *models.Order) error
}

type Channel interface {
	ServeWS() http.HandlerFunc
}

func (app *Server) OrderRequest(writer http.ResponseWriter, req *http.Request) {

	var order models.Order

	if err := json.NewDecoder(req.Body).Decode(&order); err != nil {
		log.Printf("Error decoding body: %v", err)
		http.Error(writer, "order request failure", http.StatusInternalServerError)
		return
	}
	err := app.service.PlaceRequest(&order)

	if err != nil {
		log.Printf("Error placing order: %v", err)
		http.Error(writer, "order request failure", http.StatusInternalServerError)
		return
	}

	if err := WriteJSON(writer, http.StatusCreated, order); err != nil {
		log.Printf("Error writing response: %v", err)
		http.Error(writer, "order request failure", http.StatusInternalServerError)
	}
}

func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(status)

	return json.NewEncoder(w).Encode(v)
}
