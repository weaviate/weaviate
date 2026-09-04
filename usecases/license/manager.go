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

package license

import (
	"context"
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	"github.com/weaviate/weaviate/usecases/license/protocol"
)

// Manager owns the node's license checker.
type Manager struct {
	cfg     Config
	checker *protocol.Checker
	logger  logrus.FieldLogger
	status  *prometheus.GaugeVec
	expires prometheus.Gauge
}

// Deps are the host-provided inputs.
type Deps struct {
	NodeName   string        // stable per node, reported as instance_id
	ClusterID  func() string // raft cluster id, may be "" early in startup
	Version    string        // Weaviate server version
	Logger     logrus.FieldLogger
	Registerer prometheus.Registerer
}

// New builds a Manager. With an empty key it is a no-op community-mode
// manager that never contacts the license service.
func New(cfg Config, d Deps) (*Manager, error) {
	m := &Manager{cfg: cfg, logger: d.Logger.WithField("action", "license")}
	m.registerMetrics(d.Registerer)

	if !cfg.Enabled() {
		m.checker = &protocol.Checker{}
		m.checker.Start()
		m.observe(m.checker.Snapshot())
		return m, nil
	}
	trusted, err := TrustedServerKeys(cfg)
	if err != nil {
		return nil, err
	}
	if len(trusted) == 0 {
		// Fail closed: with no trusted keys nothing the server says can be
		// believed. Running is still allowed (log-only or within grace).
		m.logger.Error("no trusted license server keys in this build; license cannot be verified")
	}
	client, err := protocol.NewClient(cfg.Key, trusted)
	if err != nil {
		return nil, err
	}
	client.ServerURL = cfg.ServerURL
	client.UserAgent = "weaviate/" + d.Version

	clusterID := d.ClusterID
	if cfg.ClusterID != "" {
		fixed := cfg.ClusterID
		clusterID = func() string { return fixed }
	}
	m.checker = &protocol.Checker{
		Client:          client,
		ClusterIDFunc:   clusterID,
		InstanceID:      d.NodeName,
		WeaviateVersion: d.Version,
		CachePath:       cfg.CachePath,
		GracePeriod:     cfg.GracePeriod,
		Enforce:         cfg.Enforce,
		Log:             newSlogLogger(m.logger),
		OnChange:        func(_, n protocol.Snapshot) { m.observe(n) },
	}
	m.checker.Start()
	m.observe(m.checker.Snapshot())
	m.logger.WithFields(logrus.Fields{
		"license_id": client.LicenseID, "enforce": cfg.Enforce, "grace_period": cfg.GracePeriod,
		"trusted_server_keys": len(trusted),
	}).Info("license configured")
	return m, nil
}

// Run checks until ctx ends. Returns immediately in community mode.
func (m *Manager) Run(ctx context.Context) {
	m.checker.Run(ctx)
}

// Snapshot returns the current license view.
func (m *Manager) Snapshot() protocol.Snapshot { return m.checker.Snapshot() }

// Allowed reports whether enterprise features may run. It is true in
// community mode as well: gating a feature also requires the caller to
// check Snapshot().State != StateUnlicensed if the feature needs a license
// at all.
func (m *Manager) Allowed() bool { return m.checker.Allowed() }

// ErrDegraded is returned by Require when enforcement has disabled
// enterprise features.
var ErrDegraded = errors.New("license degraded: this feature requires a valid Weaviate license; contact Weaviate support")

// Require is the gate for enterprise features: nil when the node may run
// them, ErrDegraded otherwise.
func (m *Manager) Require() error {
	if m.Allowed() {
		return nil
	}
	return ErrDegraded
}

// MetaInfo is the license section of GET /v1/meta. Nothing secret: the
// license ID is the public half of the key and identifies the cluster's
// license in support conversations.
func (m *Manager) MetaInfo() map[string]interface{} {
	s := m.Snapshot()
	info := map[string]interface{}{"status": string(s.State)}
	if s.State == protocol.StateUnlicensed {
		return info
	}
	info["licenseId"] = s.LicenseID
	info["enforcing"] = s.Enforcing
	if !s.ExpiresAt.IsZero() {
		info["expiresAt"] = s.ExpiresAt.UTC().Format(time.RFC3339)
	}
	if !s.LastCheckedAt.IsZero() {
		info["lastCheckedAt"] = s.LastCheckedAt.UTC().Format(time.RFC3339)
	}
	if !s.GraceEndsAt.IsZero() && s.State != protocol.StateValid {
		info["graceEndsAt"] = s.GraceEndsAt.UTC().Format(time.RFC3339)
	}
	if s.ClusterMismatch {
		info["clusterMismatch"] = true
	}
	return info
}

var allStates = []protocol.State{
	protocol.StateUnlicensed, protocol.StateValid, protocol.StateExpired, protocol.StateRevoked,
	protocol.StateUnknownLicense, protocol.StateUnreachable, protocol.StateDegraded,
}

func (m *Manager) registerMetrics(reg prometheus.Registerer) {
	if reg == nil {
		return
	}
	m.status = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "weaviate_license_status",
		Help: "License state of this node; 1 for the current state, 0 otherwise.",
	}, []string{"status"})
	m.expires = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "weaviate_license_expires_at_seconds",
		Help: "Unix time at which the license expires; 0 when unlicensed or unknown.",
	})
	for _, c := range []prometheus.Collector{m.status, m.expires} {
		if err := reg.Register(c); err != nil {
			var already prometheus.AlreadyRegisteredError
			if errors.As(err, &already) {
				// Same metric registered by an earlier Manager (tests); reuse it.
				switch existing := already.ExistingCollector.(type) {
				case *prometheus.GaugeVec:
					m.status = existing
				case prometheus.Gauge:
					m.expires = existing
				}
				continue
			}
			m.logger.WithError(err).Warn("could not register license metrics")
			m.status, m.expires = nil, nil
			return
		}
	}
}

func (m *Manager) observe(s protocol.Snapshot) {
	if m.status == nil {
		return
	}
	for _, st := range allStates {
		v := 0.0
		if st == s.State {
			v = 1
		}
		m.status.WithLabelValues(string(st)).Set(v)
	}
	if s.ExpiresAt.IsZero() {
		m.expires.Set(0)
	} else {
		m.expires.Set(float64(s.ExpiresAt.Unix()))
	}
}
