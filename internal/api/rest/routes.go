package rest

import (
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"net/http"
)

func (app *Server) loadChiRoutes() *chi.Mux {
	router := chi.NewRouter()

	router.Use(middleware.Logger)

	router.Get("/", func(writer http.ResponseWriter, reader *http.Request) {
		writer.WriteHeader(http.StatusOK)
	})

	router.Route("/orders", app.loadUserRoutes)

	return router
}

func (app *Server) loadUserRoutes(router chi.Router) {

	router.Post("/", app.Create)
	router.Get("/", app.Cancel)
}
