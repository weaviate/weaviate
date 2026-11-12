//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replication

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/go-openapi/runtime/middleware"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/replication"
	rebalance "github.com/weaviate/weaviate/cluster/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const rebalanceLogAction = "replication_rebalance"

var (
	errRebalanceDisabled     = errors.New("replica movement and replica rebalancing must both be enabled")
	errApproveNotImplemented = errors.New("approving a rebalance plan is not implemented")
	errNoRebalancePlan       = errors.New("no rebalance plan exists")
)

// rebalanceManager proposes and reads the cluster's rebalance plan
type rebalanceManager interface {
	CreateRebalancePlan(ctx context.Context, req *rebalance.CreatePlanRequest) error
	StopRebalancePlan(ctx context.Context, req *rebalance.StopPlanRequest) error
	DeleteRebalancePlan(ctx context.Context, req *rebalance.DeletePlanRequest) error
	// ReadRebalancePlan reads this node's local state, which can lag the leader.
	ReadRebalancePlan() (rebalance.Plan, bool)
	// QueryRebalancePlan retrieves the leader's state. It returns nil and no error when the leader holds no plan.
	QueryRebalancePlan(ctx context.Context) (*rebalance.Plan, error)
	// RebalanceDraftTasks reads this node's FSM, which can lag the leader.
	RebalanceDraftTasks() []rebalance.DraftTask
}

// rebalanceNodes reports cluster membership
type rebalanceNodes interface {
	// StorageCandidates returns the live members that are storage nodes.
	StorageCandidates() []string
	// NonStorageNodes returns the nodes configured as non-storage.
	NonStorageNodes() []string
}

func (h *replicationHandler) authorizeRebalance(ctx context.Context, principal *models.Principal, verb string) error {
	return h.authorizer.Authorize(ctx, principal, verb, authorization.Replications("*", "*"))
}

func (h *replicationHandler) createRebalancePlan(params replication.CreateRebalancePlanParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	if err := h.authorizeRebalance(ctx, principal, authorization.CREATE); err != nil {
		return replication.NewCreateRebalancePlanForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if !h.enabled.Get() || !h.rebalanceEnabled.Get() {
		return replication.NewCreateRebalancePlanNotImplemented().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errRebalanceDisabled))
	}
	if err := validateRebalanceRequest(params.Body); err != nil {
		return replication.NewCreateRebalancePlanBadRequest().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if errs := h.unavailableRebalanceNodes(params.Body); len(errs) > 0 {
		payload := &models.ErrorResponse{}
		for _, err := range errs {
			payload.Error = append(payload.Error, cerrors.ErrPayloadFromSingleErr(principal, err).Error...)
		}
		return replication.NewCreateRebalancePlanUnprocessableEntity().WithPayload(payload)
	}

	id, err := uuid.NewRandom()
	if err != nil {
		return replication.NewCreateRebalancePlanInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, fmt.Errorf("could not generate uuid v4: %w", err)))
	}
	req := &rebalance.CreatePlanRequest{
		PlanID:          id.String(),
		SourceNodes:     params.Body.SourceNodes,
		TargetNodes:     params.Body.TargetNodes,
		Collections:     params.Body.Collections,
		CreatedAtUnixMs: time.Now().UnixMilli(),
	}
	if err := h.rebalanceManager.CreateRebalancePlan(ctx, req); err != nil {
		if errors.Is(err, rebalance.ErrPlanExists) {
			return replication.NewCreateRebalancePlanConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
		return replication.NewCreateRebalancePlanInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}

	h.logger.WithFields(logrus.Fields{
		"action":      rebalanceLogAction,
		"op":          "create_rebalance_plan",
		"planId":      req.PlanID,
		"sourceNodes": req.SourceNodes,
		"targetNodes": req.TargetNodes,
	}).Info("rebalance plan created")

	return replication.NewCreateRebalancePlanAccepted().WithPayload(&models.ReplicationRebalanceCreateResponse{ID: req.PlanID})
}

// validateRebalanceRequest rejects a missing body and empty, repeating or
// overlapping node lists. The spec leaves the body optional and unconstrained,
// so the binder lets all of these through.
func validateRebalanceRequest(body *models.ReplicationRebalanceRequest) error {
	if body == nil {
		return errors.New("request body is required")
	}
	if len(body.SourceNodes) == 0 {
		return errors.New("sourceNodes must not be empty")
	}
	if len(body.TargetNodes) == 0 {
		return errors.New("targetNodes must not be empty")
	}
	if err := uniqueNodes("sourceNodes", body.SourceNodes); err != nil {
		return err
	}
	if err := uniqueNodes("targetNodes", body.TargetNodes); err != nil {
		return err
	}
	for _, node := range body.TargetNodes {
		if slices.Contains(body.SourceNodes, node) {
			return fmt.Errorf("node %q is in both sourceNodes and targetNodes", node)
		}
	}
	return nil
}

func uniqueNodes(field string, nodes []string) error {
	seen := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if _, ok := seen[node]; ok {
			return fmt.Errorf("%s repeats node %q", field, node)
		}
		seen[node] = struct{}{}
	}
	return nil
}

// unavailableRebalanceNodes returns one error per listed node that is not a
// live storage node, in request order.
func (h *replicationHandler) unavailableRebalanceNodes(body *models.ReplicationRebalanceRequest) []error {
	live := h.nodes.StorageCandidates()
	nonStorage := h.nodes.NonStorageNodes()
	var errs []error
	for _, node := range slices.Concat(body.SourceNodes, body.TargetNodes) {
		switch {
		case slices.Contains(nonStorage, node):
			errs = append(errs, fmt.Errorf("node %q is not a storage node", node))
		case !slices.Contains(live, node):
			errs = append(errs, fmt.Errorf("node %q is not a live storage node", node))
		}
	}
	return errs
}

func (h *replicationHandler) getRebalancePlan(params replication.GetRebalancePlanParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	if err := h.authorizeRebalance(ctx, principal, authorization.READ); err != nil {
		return replication.NewGetRebalancePlanForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if params.Consistency != nil && *params.Consistency {
		plan, err := h.rebalanceManager.QueryRebalancePlan(ctx)
		if err != nil {
			return replication.NewGetRebalancePlanInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
		if plan == nil {
			return replication.NewGetRebalancePlanNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errNoRebalancePlan))
		}
		return replication.NewGetRebalancePlanOK().WithPayload(rebalancePlanResponse(*plan, h.latestDraftTask(plan.ID)))
	}
	plan, ok := h.rebalanceManager.ReadRebalancePlan()
	if !ok {
		return replication.NewGetRebalancePlanNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errNoRebalancePlan))
	}
	return replication.NewGetRebalancePlanOK().WithPayload(rebalancePlanResponse(plan, h.latestDraftTask(plan.ID)))
}

// latestDraftTask returns the highest-version draft task of planID in this
// node's FSM, or nil when it holds none.
func (h *replicationHandler) latestDraftTask(planID string) *rebalance.DraftTask {
	var latest *rebalance.DraftTask
	for _, task := range h.rebalanceManager.RebalanceDraftTasks() {
		if task.PlanID == planID && (latest == nil || task.Version > latest.Version) {
			latest = &task
		}
	}
	return latest
}

// draftingStatus maps a node's draft unit status to the status GET reports.
// A node with no unit in the task shows DRAFTING.
func draftingStatus(unit rebalance.DraftUnitStatus) string {
	switch unit {
	case rebalance.DraftUnitCompleted:
		return models.ReplicationRebalancePlanDraftingItems0StatusREPORTED
	case rebalance.DraftUnitFailed:
		return models.ReplicationRebalancePlanDraftingItems0StatusFAILED
	default:
		return models.ReplicationRebalancePlanDraftingItems0StatusDRAFTING
	}
}

// rebalancePlanResponse builds the GET payload for plan. Drafting lists the
// source and target nodes only while plan is PLANNING and task is plan's
// draft task. Drafting is never nil, so its JSON is [] and not null.
func rebalancePlanResponse(plan rebalance.Plan, task *rebalance.DraftTask) *models.ReplicationRebalancePlan {
	drafting := make([]*models.ReplicationRebalancePlanDraftingItems0, 0)
	if plan.State == rebalance.PlanStatePlanning && task != nil {
		for _, node := range slices.Concat(plan.SourceNodes, plan.TargetNodes) {
			drafting = append(drafting, &models.ReplicationRebalancePlanDraftingItems0{
				Node:   node,
				Status: draftingStatus(task.Units[node]),
			})
		}
	}
	nodes := make([]*models.ReplicationRebalancePlanNodesItems0, len(plan.Nodes))
	for i, n := range plan.Nodes {
		nodes[i] = &models.ReplicationRebalancePlanNodesItems0{
			Node:                  n.Node,
			DiskTotalBytes:        n.DiskTotalBytes,
			DiskAvailableBytes:    n.DiskAvailableBytes,
			DiskWarningPercentage: n.DiskWarningPercentage,
			MemoryLimitBytes:      n.MemoryLimitBytes,
			ReplicationWorkers:    int64(n.ReplicationWorkers),
			Error:                 n.Error,
		}
	}
	baseline := make([]*models.ReplicationRebalancePlanBaselineItems0, len(plan.Baseline))
	for i, b := range plan.Baseline {
		baseline[i] = &models.ReplicationRebalancePlanBaselineItems0{
			Node:            b.Node,
			DiskUtilisation: b.DiskUtilisation,
			ReplicaCount:    int64(b.ReplicaCount),
		}
	}
	return &models.ReplicationRebalancePlan{
		ID:               plan.ID,
		State:            string(plan.State),
		SourceNodes:      plan.SourceNodes,
		TargetNodes:      plan.TargetNodes,
		Collections:      plan.Collections,
		CreatedAtUnixMs:  plan.CreatedAtUnixMs,
		MeasuredAtUnixMs: plan.MeasuredAtUnixMs,
		Reason:           plan.Reason,
		Nodes:            nodes,
		Baseline:         baseline,
		Drafting:         drafting,
	}
}

func (h *replicationHandler) stopRebalancePlan(params replication.StopRebalancePlanParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	if err := h.authorizeRebalance(ctx, principal, authorization.UPDATE); err != nil {
		return replication.NewStopRebalancePlanForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	plan, err := h.rebalanceManager.QueryRebalancePlan(ctx)
	if err != nil {
		return replication.NewStopRebalancePlanInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if plan == nil {
		return replication.NewStopRebalancePlanNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errNoRebalancePlan))
	}
	if err := h.rebalanceManager.StopRebalancePlan(ctx, &rebalance.StopPlanRequest{PlanID: plan.ID}); err != nil {
		switch {
		case errors.Is(err, rebalance.ErrPlanNotFound):
			return replication.NewStopRebalancePlanNotFound().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		case errors.Is(err, rebalance.ErrPlanTransition):
			return replication.NewStopRebalancePlanConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		default:
			return replication.NewStopRebalancePlanInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
	}

	h.logger.WithFields(logrus.Fields{
		"action": rebalanceLogAction,
		"op":     "stop_rebalance_plan",
		"planId": plan.ID,
	}).Info("rebalance plan stopped")

	return replication.NewStopRebalancePlanAccepted()
}

func (h *replicationHandler) approveRebalancePlan(params replication.ApproveRebalancePlanParams, principal *models.Principal) middleware.Responder {
	if err := h.authorizeRebalance(params.HTTPRequest.Context(), principal, authorization.CREATE); err != nil {
		return replication.NewApproveRebalancePlanForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	return replication.NewApproveRebalancePlanNotImplemented().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, errApproveNotImplemented))
}

func (h *replicationHandler) deleteRebalancePlan(params replication.DeleteRebalancePlanParams, principal *models.Principal) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	if err := h.authorizeRebalance(ctx, principal, authorization.DELETE); err != nil {
		return replication.NewDeleteRebalancePlanForbidden().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	plan, err := h.rebalanceManager.QueryRebalancePlan(ctx)
	if err != nil {
		return replication.NewDeleteRebalancePlanInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
	}
	if plan == nil {
		return replication.NewDeleteRebalancePlanNoContent()
	}
	if err := h.rebalanceManager.DeleteRebalancePlan(ctx, &rebalance.DeletePlanRequest{PlanID: plan.ID}); err != nil {
		switch {
		case errors.Is(err, rebalance.ErrPlanNotFound):
			return replication.NewDeleteRebalancePlanNoContent()
		case errors.Is(err, rebalance.ErrPlanTransition):
			return replication.NewDeleteRebalancePlanConflict().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		default:
			return replication.NewDeleteRebalancePlanInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
		}
	}

	h.logger.WithFields(logrus.Fields{
		"action": rebalanceLogAction,
		"op":     "delete_rebalance_plan",
		"planId": plan.ID,
	}).Info("rebalance plan deleted")

	return replication.NewDeleteRebalancePlanNoContent()
}
