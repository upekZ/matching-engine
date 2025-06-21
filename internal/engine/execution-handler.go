package engine

import (
	"github.com/upekZ/matching-engine/internal/models"
	"sync"
)

type ExecHandler struct {
	mu    sync.Mutex
	execs []*models.Execution
}

func NewExecHandler(execs []*models.Execution) *ExecHandler {
	return &ExecHandler{
		execs: execs,
		mu:    sync.Mutex{},
	}
}

func (p *ExecHandler) AddExecution(exec *models.Execution) {
	p.mu.Lock()
	p.execs = append(p.execs, exec)
	p.mu.Unlock()
}
