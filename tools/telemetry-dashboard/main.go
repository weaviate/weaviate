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

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

//  Local Telemetry Dashboard
//  This tool provides a local web dashboard to receive and display
//  telemetry data from Weaviate instances.
//
//  Usage:
//    go run tools/telemetry-dashboard/main.go
//    Then configure Weaviate to send telemetry to: http://localhost:9696/weaviate-telemetry

const (
	port         = ":9696"
	telemetryURL = "/weaviate-telemetry"
	dashboardURL = "/"
)

// TelemetryPayload represents the telemetry data structure
type TelemetryPayload struct {
	MachineID              string                      `json:"machineId"`
	Type                   string                      `json:"type"`
	Version                string                      `json:"version"`
	ObjectsCount           int64                       `json:"objs"`
	OS                     string                      `json:"os"`
	Arch                   string                      `json:"arch"`
	UsedModules            []string                    `json:"usedModules,omitempty"`
	CollectionsCount       int                         `json:"collectionsCount"`
	ClientUsage            map[string]map[string]int64 `json:"clientUsage,omitempty"`
	ClientIntegrationUsage map[string]map[string]int64 `json:"clientIntegrationUsage,omitempty"`
	ReceivedAt             time.Time                   `json:"receivedAt"`
}

// Dashboard stores telemetry data
type Dashboard struct {
	mu       sync.RWMutex
	payloads []*TelemetryPayload
	machines map[string]*MachineStats
	maxItems int
}

// MachineStats aggregates statistics per machine
type MachineStats struct {
	MachineID              string                      `json:"machineId"`
	FirstSeen              time.Time                   `json:"firstSeen"`
	LastSeen               time.Time                   `json:"lastSeen"`
	Version                string                      `json:"version"`
	OS                     string                      `json:"os"`
	Arch                   string                      `json:"arch"`
	TotalPayloads          int                         `json:"totalPayloads"`
	TotalObjects           int64                       `json:"totalObjects"`
	CollectionsCount       int                         `json:"collectionsCount"`
	UsedModules            map[string]bool             `json:"usedModules"`
	ClientUsage            map[string]map[string]int64 `json:"clientUsage"`
	ClientIntegrationUsage map[string]map[string]int64 `json:"clientIntegrationUsage"`
	PayloadTypes           map[string]int              `json:"payloadTypes"`
}

func NewDashboard(maxItems int) *Dashboard {
	return &Dashboard{
		payloads: make([]*TelemetryPayload, 0, maxItems),
		machines: make(map[string]*MachineStats),
		maxItems: maxItems,
	}
}

func (d *Dashboard) AddPayload(payload *TelemetryPayload) {
	d.mu.Lock()
	defer d.mu.Unlock()

	payload.ReceivedAt = time.Now()

	// Add to recent payloads list
	d.payloads = append(d.payloads, payload)
	if len(d.payloads) > d.maxItems {
		d.payloads = d.payloads[1:]
	}

	// Update machine statistics
	machineID := payload.MachineID
	stats, exists := d.machines[machineID]
	if !exists {
		stats = &MachineStats{
			MachineID:              machineID,
			FirstSeen:              payload.ReceivedAt,
			UsedModules:            make(map[string]bool),
			ClientUsage:            make(map[string]map[string]int64),
			ClientIntegrationUsage: make(map[string]map[string]int64),
			PayloadTypes:           make(map[string]int),
		}
		d.machines[machineID] = stats
	}

	stats.LastSeen = payload.ReceivedAt
	stats.TotalPayloads++
	stats.PayloadTypes[payload.Type]++
	stats.Version = payload.Version
	stats.OS = payload.OS
	stats.Arch = payload.Arch
	stats.CollectionsCount = payload.CollectionsCount

	if payload.ObjectsCount > 0 {
		stats.TotalObjects = payload.ObjectsCount
	}

	for _, module := range payload.UsedModules {
		stats.UsedModules[module] = true
	}

	for clientType, versions := range payload.ClientUsage {
		if stats.ClientUsage[clientType] == nil {
			stats.ClientUsage[clientType] = make(map[string]int64)
		}
		for version, count := range versions {
			stats.ClientUsage[clientType][version] += count
		}
	}

	for integration, versions := range payload.ClientIntegrationUsage {
		if stats.ClientIntegrationUsage[integration] == nil {
			stats.ClientIntegrationUsage[integration] = make(map[string]int64)
		}
		for version, count := range versions {
			stats.ClientIntegrationUsage[integration][version] += count
		}
	}
}

func (d *Dashboard) GetData() ([]*TelemetryPayload, map[string]*MachineStats) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Return copies. A shallow copy of *MachineStats is not enough — its
	// map fields are reference types, so a concurrent AddPayload that
	// inserts into e.g. stats.ClientUsage[type] would race with the
	// renderer (or JSON encoder) iterating the same map after GetData
	// returns and releases the read lock.
	payloads := make([]*TelemetryPayload, len(d.payloads))
	copy(payloads, d.payloads)

	machines := make(map[string]*MachineStats, len(d.machines))
	for k, v := range d.machines {
		machines[k] = copyMachineStats(v)
	}

	return payloads, machines
}

// copyMachineStats returns a deep copy of m so callers can iterate its
// map fields without holding d.mu against concurrent writers.
func copyMachineStats(m *MachineStats) *MachineStats {
	out := *m
	out.UsedModules = make(map[string]bool, len(m.UsedModules))
	for k, v := range m.UsedModules {
		out.UsedModules[k] = v
	}
	out.PayloadTypes = make(map[string]int, len(m.PayloadTypes))
	for k, v := range m.PayloadTypes {
		out.PayloadTypes[k] = v
	}
	out.ClientUsage = copyCountMap(m.ClientUsage)
	out.ClientIntegrationUsage = copyCountMap(m.ClientIntegrationUsage)
	return &out
}

func copyCountMap(m map[string]map[string]int64) map[string]map[string]int64 {
	out := make(map[string]map[string]int64, len(m))
	for k, inner := range m {
		dup := make(map[string]int64, len(inner))
		for ik, iv := range inner {
			dup[ik] = iv
		}
		out[k] = dup
	}
	return out
}

func main() {
	dashboard := NewDashboard(100)

	// Telemetry endpoint
	http.HandleFunc(telemetryURL, func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to read body: %v", err), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		var payload TelemetryPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, fmt.Sprintf("Failed to parse JSON: %v", err), http.StatusBadRequest)
			return
		}

		dashboard.AddPayload(&payload)
		log.Printf("Received %s payload from machine %s (version %s)", payload.Type, payload.MachineID, payload.Version)

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Dashboard endpoint
	http.HandleFunc(dashboardURL, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != dashboardURL {
			http.NotFound(w, r)
			return
		}

		// The dashboard markup is static; refreshData() populates
		// #payloads and #machines from /api/data on load and every
		// 2 seconds thereafter.
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(generateDashboardHTML()))
	})

	// API endpoint for JSON data (for auto-refresh)
	http.HandleFunc("/api/data", func(w http.ResponseWriter, r *http.Request) {
		payloads, machines := dashboard.GetData()
		data := map[string]interface{}{
			"payloads": payloads,
			"machines": machines,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	})

	log.Printf("Telemetry Dashboard starting on http://localhost%s", port)
	log.Printf("Configure Weaviate to send telemetry to: http://localhost%s%s", port, telemetryURL)
	log.Printf("View dashboard at: http://localhost%s", port)
	log.Fatal(http.ListenAndServe(port, nil))
}

func generateDashboardHTML() string {
	return fmt.Sprintf(`<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Weaviate Telemetry Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%%, #764ba2 100%%);
            color: #333;
            padding: 20px;
            min-height: 100vh;
        }
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        header {
            background: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        h1 {
            color: #667eea;
            margin-bottom: 10px;
        }
        .status {
            display: inline-block;
            padding: 5px 15px;
            background: #10b981;
            color: white;
            border-radius: 20px;
            font-size: 14px;
            margin-top: 10px;
        }
        .grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .card h2 {
            color: #667eea;
            margin-bottom: 15px;
            font-size: 20px;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }
        .machine {
            margin-bottom: 20px;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }
        .machine-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 10px;
        }
        .machine-id {
            font-family: monospace;
            font-size: 12px;
            color: #666;
        }
        .badge {
            display: inline-block;
            padding: 3px 8px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: bold;
            margin: 2px;
        }
        .badge-init { background: #3b82f6; color: white; }
        .badge-update { background: #10b981; color: white; }
        .badge-terminate { background: #ef4444; color: white; }
        .badge-client { background: #8b5cf6; color: white; }
        .stats {
            display: grid;
            grid-template-columns: repeat(2, 1fr);
            gap: 10px;
            margin-top: 10px;
        }
        .stat-item {
            padding: 8px;
            background: white;
            border-radius: 5px;
        }
        .stat-label {
            font-size: 11px;
            color: #666;
            text-transform: uppercase;
        }
        .stat-value {
            font-size: 18px;
            font-weight: bold;
            color: #333;
        }
        .payload-list {
            max-height: 400px;
            overflow-y: auto;
        }
        .payload-item {
            padding: 10px;
            margin-bottom: 10px;
            background: #f8f9fa;
            border-radius: 5px;
            border-left: 3px solid #667eea;
        }
        .payload-header {
            display: flex;
            justify-content: space-between;
            margin-bottom: 5px;
        }
        .payload-time {
            font-size: 11px;
            color: #666;
        }
        .client-usage {
            margin-top: 10px;
        }
        .client-item {
            display: inline-block;
            padding: 5px 10px;
            margin: 3px;
            background: #e0e7ff;
            border-radius: 5px;
            font-size: 12px;
        }
        .empty {
            text-align: center;
            color: #999;
            padding: 40px;
        }
        .refresh-info {
            text-align: center;
            color: #666;
            font-size: 12px;
            margin-top: 10px;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <h1>🔍 Weaviate Telemetry Dashboard</h1>
            <div class="status">● Live</div>
            <p style="margin-top: 10px; color: #666;">
                Receiving telemetry data from Weaviate instances. 
                Configure instances to send telemetry to: <code>http://localhost%s%s</code>
            </p>
        </header>

        <div class="grid">
            <div class="card">
                <h2>📊 Recent Payloads</h2>
                <div class="payload-list" id="payloads">
                    <div class="empty">Loading…</div>
                </div>
            </div>

            <div class="card">
                <h2>🖥️ Machines</h2>
                <div id="machines">
                    <div class="empty">Loading…</div>
                </div>
            </div>
        </div>

        <div class="refresh-info">Auto-refreshing every 2 seconds...</div>
    </div>

    <script>
        // escapeHtml escapes user-controlled values (integration names, versions,
        // client SDK identifiers) before they are concatenated into HTML, so a
        // crafted telemetry payload cannot execute markup in the dashboard.
        function escapeHtml(s) {
            if (s === null || s === undefined) return '';
            return String(s)
                .replace(/&/g, '&amp;')
                .replace(/</g, '&lt;')
                .replace(/>/g, '&gt;')
                .replace(/"/g, '&quot;')
                .replace(/'/g, '&#39;');
        }

        function formatTime(timeStr) {
            if (!timeStr) return 'N/A';
            const date = new Date(timeStr);
            return date.toLocaleString();
        }

        function formatRelativeTime(timeStr) {
            if (!timeStr) return 'N/A';
            const date = new Date(timeStr);
            const now = new Date();
            const diff = Math.floor((now - date) / 1000);
            if (diff < 60) return diff + 's ago';
            if (diff < 3600) return Math.floor(diff / 60) + 'm ago';
            return Math.floor(diff / 3600) + 'h ago';
        }

        function renderPayloads(payloads) {
            if (payloads.length === 0) {
                return '<div class="empty">No telemetry data received yet</div>';
            }
            return payloads.slice().reverse().map(function(p) {
                const clientUsage = p.clientUsage || {};
                const clientHtml = Object.keys(clientUsage).map(function(type) {
                    const versions = clientUsage[type] || {};
                    // The client SDK type is constrained to a fixed enum by the
                    // server-side parser, but the version is taken verbatim
                    // from the X-Weaviate-Client header and must be escaped.
                    const versionItems = Object.keys(versions).map(function(version) {
                        return escapeHtml(version) + ' (' + versions[version] + ')';
                    }).join(', ');
                    return '<span class="client-item"><strong>' + type + ':</strong> ' + versionItems + '</span>';
                }).join('');
                const integrationUsage = p.clientIntegrationUsage || {};
                const integrationHtml = Object.keys(integrationUsage).map(function(name) {
                    const versions = integrationUsage[name] || {};
                    const versionItems = Object.keys(versions).map(function(version) {
                        return escapeHtml(version) + ' (' + versions[version] + ')';
                    }).join(', ');
                    return '<span class="client-item" style="background:#d1fae5;"><strong>' + escapeHtml(name) + ':</strong> ' + versionItems + '</span>';
                }).join('');
                var html = '<div class="payload-item">' +
                    '<div class="payload-header">' +
                    '<span class="badge badge-' + p.type.toLowerCase() + '">' + p.type + '</span>' +
                    '<span class="payload-time">' + formatRelativeTime(p.receivedAt) + '</span>' +
                    '</div>' +
                    '<div><strong>Machine:</strong> <span class="machine-id">' + p.machineId + '</span></div>' +
                    '<div><strong>Version:</strong> ' + p.version + '</div>' +
                    '<div><strong>OS/Arch:</strong> ' + p.os + '/' + p.arch + '</div>';
                if (p.objs > 0) {
                    html += '<div><strong>Objects:</strong> ' + p.objs.toLocaleString() + '</div>';
                }
                if (p.collectionsCount > 0) {
                    html += '<div><strong>Collections:</strong> ' + p.collectionsCount + '</div>';
                }
                if (clientHtml) {
                    html += '<div class="client-usage"><strong>Client Usage:</strong><br>' + clientHtml + '</div>';
                }
                if (integrationHtml) {
                    html += '<div class="client-usage"><strong>Integration Usage:</strong><br>' + integrationHtml + '</div>';
                }
                if (p.usedModules && p.usedModules.length > 0) {
                    html += '<div><strong>Modules:</strong> ' + p.usedModules.join(', ') + '</div>';
                }
                html += '</div>';
                return html;
            }).join('');
        }

        function renderMachines(machines) {
            if (Object.keys(machines).length === 0) {
                return '<div class="empty">No machines registered yet</div>';
            }
            return Object.keys(machines).map(function(key) {
                var m = machines[key];
                const modules = Object.keys(m.usedModules || {});
                const clientUsage = m.clientUsage || {};
                const payloadTypes = Object.keys(m.payloadTypes || {}).map(function(type) {
                    return '<span class="badge badge-' + type.toLowerCase() + '">' + type + ': ' + m.payloadTypes[type] + '</span>';
                }).join(' ');
                const clientHtml = Object.keys(clientUsage).map(function(type) {
                    const versions = clientUsage[type] || {};
                    var totalCount = 0;
                    // See renderPayloads: type is enum-constrained, version is
                    // user-controlled and must be escaped.
                    const versionDetails = Object.keys(versions).map(function(version) {
                        totalCount += versions[version];
                        return escapeHtml(version) + ' (' + versions[version] + ')';
                    }).join(', ');
                    return '<span class="client-item"><strong>' + type + ':</strong> ' + totalCount + ' total (' + versionDetails + ')</span>';
                }).join('');
                var html = '<div class="machine">' +
                    '<div class="machine-header">' +
                    '<div>' +
                    '<div class="machine-id">' + m.machineId + '</div>' +
                    '<div style="margin-top: 5px;">' + payloadTypes + '</div>' +
                    '</div>' +
                    '</div>' +
                    '<div class="stats">' +
                    '<div class="stat-item">' +
                    '<div class="stat-label">Version</div>' +
                    '<div class="stat-value">' + (m.version || 'N/A') + '</div>' +
                    '</div>' +
                    '<div class="stat-item">' +
                    '<div class="stat-label">OS/Arch</div>' +
                    '<div class="stat-value">' + m.os + '/' + m.arch + '</div>' +
                    '</div>' +
                    '<div class="stat-item">' +
                    '<div class="stat-label">Total Payloads</div>' +
                    '<div class="stat-value">' + m.totalPayloads + '</div>' +
                    '</div>' +
                    '<div class="stat-item">' +
                    '<div class="stat-label">Objects</div>' +
                    '<div class="stat-value">' + m.totalObjects.toLocaleString() + '</div>' +
                    '</div>' +
                    '<div class="stat-item">' +
                    '<div class="stat-label">Collections</div>' +
                    '<div class="stat-value">' + m.collectionsCount + '</div>' +
                    '</div>' +
                    '<div class="stat-item">' +
                    '<div class="stat-label">Modules</div>' +
                    '<div class="stat-value">' + modules.length + '</div>' +
                    '</div>' +
                    '</div>';
                const integrationUsage = m.clientIntegrationUsage || {};
                const integrationHtml = Object.keys(integrationUsage).map(function(name) {
                    const versions = integrationUsage[name] || {};
                    var totalCount = 0;
                    const versionDetails = Object.keys(versions).map(function(version) {
                        totalCount += versions[version];
                        return escapeHtml(version) + ' (' + versions[version] + ')';
                    }).join(', ');
                    return '<span class="client-item" style="background:#d1fae5;"><strong>' + escapeHtml(name) + ':</strong> ' + totalCount + ' total (' + versionDetails + ')</span>';
                }).join('');
                if (modules.length > 0) {
                    html += '<div style="margin-top: 10px;"><strong>Modules:</strong> ' + modules.join(', ') + '</div>';
                }
                if (clientHtml) {
                    html += '<div class="client-usage" style="margin-top: 10px;"><strong>Total Client Usage:</strong><br>' + clientHtml + '</div>';
                }
                if (integrationHtml) {
                    html += '<div class="client-usage" style="margin-top: 10px;"><strong>Total Integration Usage:</strong><br>' + integrationHtml + '</div>';
                }
                html += '<div style="margin-top: 10px; font-size: 11px; color: #666;">' +
                    'First seen: ' + formatTime(m.firstSeen) + '<br>' +
                    'Last seen: ' + formatTime(m.lastSeen) + ' (' + formatRelativeTime(m.lastSeen) + ')' +
                    '</div>' +
                    '</div>';
                return html;
            }).join('');
        }

        async function refreshData() {
            try {
                const response = await fetch('/api/data');
                const data = await response.json();
                document.getElementById('payloads').innerHTML = renderPayloads(data.payloads || []);
                document.getElementById('machines').innerHTML = renderMachines(data.machines || {});
            } catch (error) {
                console.error('Failed to refresh data:', error);
            }
        }

        // Initial load
        refreshData();
        // Auto-refresh every 2 seconds
        setInterval(refreshData, 2000);
    </script>
</body>
</html>`, port, telemetryURL)
}
