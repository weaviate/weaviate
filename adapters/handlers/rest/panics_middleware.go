package rest

import (
	"net"
	"net/http"
	"syscall"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func makeCatchPanics(logger logrus.FieldLogger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer handlePanics(logger, r)
			next.ServeHTTP(w, r)
		})
	}
}

func handlePanics(logger logrus.FieldLogger, r *http.Request) {
	recovered := recover()
	if recovered == nil {
		return
	}

	err, ok := recovered.(error)
	if !ok {
		// not a typed error, we can not handle this error other returning it to
		// the user
		logger.WithField("error", recovered).Error()
		return
	}

	if errors.Is(err, syscall.EPIPE) {
		handleBrokenPipe(err, logger, r)
		return
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		if netErr.Timeout() {
			handleTimeout(netErr, logger, r)
			return
		}
	}
}

func handleBrokenPipe(err error, logger logrus.FieldLogger, r *http.Request) {
	logger.WithError(err).WithFields(logrus.Fields{
		"method":      r.Method,
		"path":        r.URL,
		"description": "A broken pipe error occurs when Weaviate tries to write a response onto a connection that has already been closed or reset by the client. Typically, this is the case when the server was not able to respond within the configured client-side timeout.",
		"hint":        "Either try increasing the client-side timeout, or sending a computationally cheaper request, for example by reducing a batch size, reducing a limit, using less complex filters, etc.",
	}).Errorf("broken pipe")
}

func handleTimeout(err net.Error, logger logrus.FieldLogger, r *http.Request) {
	logger.WithError(err).WithFields(logrus.Fields{
		"method":      r.Method,
		"path":        r.URL,
		"description": "An I/O timeout occurs when the request takes longer than the specified server-side timeout.",
		"hint":        "Either try increasing the server-side timeout using e.g. '--write-timeout 600s' as a command line flag when starting Weaviate, or try sending a computationally cheaper request, for example by reducing a batch size, reducing a limit, using less complex filters, etc. Note that this error is only thrown if client-side and server-side timeouts are not in sync, more precisely if the client-side timeout is longer than the server side timeout.",
	}).Errorf("i/o timeout")
}
