package admin

import (
	"net/http"
	"strconv"

	"github.com/Wei-Shaw/sub2api/internal/pkg/response"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *AccountHandler) SetTurnStateService(s *service.OpenAITurnStateService) { h.turnStates = s }

func (h *AccountHandler) turnStateAccountID(c *gin.Context) (int64, bool) {
	if h.turnStates == nil {
		response.Error(c, http.StatusServiceUnavailable, "turn-state service unavailable")
		return 0, false
	}
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil || id <= 0 {
		response.Error(c, http.StatusBadRequest, "invalid account ID")
		return 0, false
	}
	return id, true
}

func (h *AccountHandler) GetTurnStateStatus(c *gin.Context) {
	id, ok := h.turnStateAccountID(c)
	if !ok {
		return
	}
	result, err := h.turnStates.Status(c.Request.Context(), id)
	if err != nil {
		response.Error(c, http.StatusServiceUnavailable, "unable to read turn-state status")
		return
	}
	response.Success(c, result)
}

func (h *AccountHandler) ProbeTurnState(c *gin.Context) {
	id, ok := h.turnStateAccountID(c)
	if !ok {
		return
	}
	var req struct {
		Model       string `json:"model"`
		ServiceTier string `json:"service_tier"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.Error(c, http.StatusBadRequest, "invalid probe target")
		return
	}
	result, err := h.turnStates.Request(c.Request.Context(), id, req.Model, req.ServiceTier)
	if err != nil {
		response.Error(c, http.StatusBadRequest, err.Error())
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"code": 0, "message": "success", "data": result})
}

func (h *AccountHandler) ClearTurnState(c *gin.Context) {
	id, ok := h.turnStateAccountID(c)
	if !ok {
		return
	}
	if err := h.turnStates.Clear(c.Request.Context(), id, c.Query("model"), c.Query("service_tier")); err != nil {
		response.Error(c, http.StatusServiceUnavailable, "unable to clear turn-state cache")
		return
	}
	response.Success(c, gin.H{"cleared": true})
}
