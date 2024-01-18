//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package rest

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

type RequestStatus int

const (
	Ok RequestStatus = iota
	UserError
	ServerError
)

func (s RequestStatus) String() string {
	switch s {
	case Ok:
		return "ok"
	case UserError:
		return "user_error"
	case ServerError:
		return "server_error"
	}
	return "unknown"
}

type requestsTotalMetric struct {
	requestsTotal *prometheus.GaugeVec
	groupClasses  bool
	api           string
}

func newRequestsTotalMetric(prom *monitoring.PrometheusMetrics, api string) *requestsTotalMetric {
	if prom == nil {
		return nil
	}
	return &requestsTotalMetric{
		requestsTotal: prom.RequestsTotal,
		groupClasses:  prom.Group,
		api:           api,
	}
}

func (m *requestsTotalMetric) RequestsTotalInc(status RequestStatus, className, queryType string) {
	if m == nil {
		return
	}

	if m.groupClasses {
		className = "n/a"
	}

	m.requestsTotal.With(prometheus.Labels{
		"status":     status.String(),
		"class_name": className,
		"api":        m.api,
		"query_type": queryType,
	}).Inc()
}

type restApiRequestsTotal interface {
	logError(className string, err error)
	logOk(className string)
	logUserError(className string)
	logServerError(className string, err error)
}

type restApiRequestsTotalImpl struct {
	metrics        *requestsTotalMetric
	api, queryType string
	logger         logrus.FieldLogger
}

func (e *restApiRequestsTotalImpl) logOk(className string) {
	if e.metrics != nil {
		e.metrics.RequestsTotalInc(Ok, className, e.queryType)
	}
}

func (e *restApiRequestsTotalImpl) logUserError(className string) {
	if e.metrics != nil {
		e.metrics.RequestsTotalInc(UserError, className, e.queryType)
	}
}

func (e *restApiRequestsTotalImpl) logServerError(className string, err error) {
	e.logger.WithFields(logrus.Fields{
		"action":     "requests_total",
		"api":        e.api,
		"query_type": e.queryType,
		"class_name": className,
	}).WithError(err).Error("unexpected error")
	if e.metrics != nil {
		e.metrics.RequestsTotalInc(ServerError, className, e.queryType)
	}
}

type panicsRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newPanicsRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &panicsRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "", logger},
	}
}

func (e *panicsRequestsTotal) logError(className string, err error) {
	e.logServerError(className, err)
}
