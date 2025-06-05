package rest

import (
	"encoding/json"
	"github.com/upekZ/matching-engine/internal/models"
	"log"
	"net/http"
)

type OrderService interface {
	PlaceRequest(order *models.Order) models.Order
}

type Channel interface {
	ServeWS() http.HandlerFunc
}

func (app *Server) NewLimitOrderRequest(writer http.ResponseWriter, req *http.Request) {
	app.NewRequest(writer, req, models.NewLimitOrder)
}

func (app *Server) CancelOrderRequest(writer http.ResponseWriter, req *http.Request) {
	app.NewRequest(writer, req, models.CancelOrder)
}

func (app *Server) NewRequest(writer http.ResponseWriter, req *http.Request, orderType models.OrderType) {

	var order models.Order

	if err := json.NewDecoder(req.Body).Decode(&order); err != nil {
		log.Printf("error decoding body: %v", err)
		http.Error(writer, "order request failure", http.StatusInternalServerError)
		return
	}
	orderResp := app.service.PlaceRequest(&order)

	if err := WriteJSON(writer, http.StatusCreated, orderResp); err != nil {
		log.Printf("Error writing response: %v", err)
		http.Error(writer, "order request failure", http.StatusInternalServerError)
	}
}

func WriteJSON(w http.ResponseWriter, status int, v any) error {
	w.Header().Add("Content-Type", "application/json")
	w.WriteHeader(status)

	return json.NewEncoder(w).Encode(v)
}
