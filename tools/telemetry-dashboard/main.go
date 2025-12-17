//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//
//  Local Telemetry Dashboard
//  This tool provides a local web dashboard to receive and display
//  telemetry data from Weaviate instances.
//
//  Usage:
//    go run tools/telemetry-dashboard/main.go
//    Then configure Weaviate to send telemetry to: http://localhost:8080/weaviate-telemetry

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

const (
	port         = ":9696"
	telemetryURL = "/weaviate-telemetry"
	dashboardURL = "/"
)

// TelemetryPayload represents the telemetry data structure
type TelemetryPayload struct {
	MachineID        string                      `json:"machineId"`
	Type             string                      `json:"type"`
	Version          string                      `json:"version"`
	ObjectsCount     int64                       `json:"objs"`
	OS               string                      `json:"os"`
	Arch             string                      `json:"arch"`
	UsedModules      []string                    `json:"usedModules,omitempty"`
	CollectionsCount int                         `json:"collectionsCount"`
	ClientUsage      map[string]map[string]int64 `json:"clientUsage,omitempty"`
	ReceivedAt       time.Time                   `json:"receivedAt"`
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
	MachineID        string                      `json:"machineId"`
	FirstSeen        time.Time                   `json:"firstSeen"`
	LastSeen         time.Time                   `json:"lastSeen"`
	Version          string                      `json:"version"`
	OS               string                      `json:"os"`
	Arch             string                      `json:"arch"`
	TotalPayloads    int                         `json:"totalPayloads"`
	TotalObjects     int64                       `json:"totalObjects"`
	CollectionsCount int                         `json:"collectionsCount"`
	UsedModules      map[string]bool             `json:"usedModules"`
	ClientUsage      map[string]map[string]int64 `json:"clientUsage"`
	PayloadTypes     map[string]int              `json:"payloadTypes"`
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
			MachineID:    machineID,
			FirstSeen:    payload.ReceivedAt,
			UsedModules:  make(map[string]bool),
			ClientUsage:  make(map[string]map[string]int64),
			PayloadTypes: make(map[string]int),
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
}

func (d *Dashboard) GetData() ([]*TelemetryPayload, map[string]*MachineStats) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	// Return copies
	payloads := make([]*TelemetryPayload, len(d.payloads))
	copy(payloads, d.payloads)

	machines := make(map[string]*MachineStats)
	for k, v := range d.machines {
		stats := *v
		machines[k] = &stats
	}

	return payloads, machines
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

		payloads, machines := dashboard.GetData()
		html := generateDashboardHTML(payloads, machines)
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(html))
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

func generateDashboardHTML(payloads []*TelemetryPayload, machines map[string]*MachineStats) string {
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
                    %s
                </div>
            </div>

            <div class="card">
                <h2>🖥️ Machines</h2>
                <div id="machines">
                    %s
                </div>
            </div>
        </div>

        <div class="refresh-info">Auto-refreshing every 2 seconds...</div>
    </div>

    <script>
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
                    const versionItems = Object.keys(versions).map(function(version) {
                        return version + ' (' + versions[version] + ')';
                    }).join(', ');
                    return '<span class="client-item"><strong>' + type + ':</strong> ' + versionItems + '</span>';
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
                    const versionDetails = Object.keys(versions).map(function(version) {
                        totalCount += versions[version];
                        return version + ' (' + versions[version] + ')';
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
                if (modules.length > 0) {
                    html += '<div style="margin-top: 10px;"><strong>Modules:</strong> ' + modules.join(', ') + '</div>';
                }
                if (clientHtml) {
                    html += '<div class="client-usage" style="margin-top: 10px;"><strong>Total Client Usage:</strong><br>' + clientHtml + '</div>';
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
</html>`, port, telemetryURL, renderPayloadsHTML(payloads), renderMachinesHTML(machines))
}

func renderPayloadsHTML(payloads []*TelemetryPayload) string {
	if len(payloads) == 0 {
		return `<div class="empty">No telemetry data received yet</div>`
	}

	html := ""
	for i := len(payloads) - 1; i >= 0; i-- {
		p := payloads[i]
		html += fmt.Sprintf(`
			<div class="payload-item">
				<div class="payload-header">
					<span class="badge badge-%s">%s</span>
					<span class="payload-time">%s</span>
				</div>
				<div><strong>Machine:</strong> <span class="machine-id">%s</span></div>
				<div><strong>Version:</strong> %s</div>
				<div><strong>OS/Arch:</strong> %s/%s</div>`,
			toLower(p.Type), p.Type, formatRelativeTime(p.ReceivedAt),
			p.MachineID, p.Version, p.OS, p.Arch)

		if p.ObjectsCount > 0 {
			html += fmt.Sprintf(`<div><strong>Objects:</strong> %s</div>`, formatNumber(p.ObjectsCount))
		}
		if p.CollectionsCount > 0 {
			html += fmt.Sprintf(`<div><strong>Collections:</strong> %d</div>`, p.CollectionsCount)
		}
		if len(p.ClientUsage) > 0 {
			html += `<div class="client-usage"><strong>Client Usage:</strong><br>`
			for clientType, versions := range p.ClientUsage {
				versionItems := make([]string, 0)
				for version, count := range versions {
					versionItems = append(versionItems, fmt.Sprintf("%s (%d)", version, count))
				}
				html += fmt.Sprintf(`<span class="client-item"><strong>%s:</strong> %s</span>`, clientType, joinStrings(versionItems, ", "))
			}
			html += `</div>`
		}
		if len(p.UsedModules) > 0 {
			html += fmt.Sprintf(`<div><strong>Modules:</strong> %s</div>`, joinStrings(p.UsedModules, ", "))
		}
		html += `</div>`
	}
	return html
}

func renderMachinesHTML(machines map[string]*MachineStats) string {
	if len(machines) == 0 {
		return `<div class="empty">No machines registered yet</div>`
	}

	html := ""
	for _, m := range machines {
		modules := make([]string, 0, len(m.UsedModules))
		for mod := range m.UsedModules {
			modules = append(modules, mod)
		}

		payloadTypesHtml := ""
		for ptype, count := range m.PayloadTypes {
			payloadTypesHtml += fmt.Sprintf(`<span class="badge badge-%s">%s: %d</span> `, toLower(ptype), ptype, count)
		}

		clientUsageHtml := ""
		if len(m.ClientUsage) > 0 {
			clientUsageHtml = `<div class="client-usage" style="margin-top: 10px;"><strong>Total Client Usage:</strong><br>`
			for clientType, versions := range m.ClientUsage {
				var totalCount int64
				versionItems := make([]string, 0)
				for version, count := range versions {
					totalCount += count
					versionItems = append(versionItems, fmt.Sprintf("%s (%d)", version, count))
				}
				clientUsageHtml += fmt.Sprintf(`<span class="client-item"><strong>%s:</strong> %d total (%s)</span>`, clientType, totalCount, joinStrings(versionItems, ", "))
			}
			clientUsageHtml += `</div>`
		}

		modulesHtml := ""
		if len(modules) > 0 {
			modulesHtml = fmt.Sprintf(`<div style="margin-top: 10px;"><strong>Modules:</strong> %s</div>`, joinStrings(modules, ", "))
		}

		html += fmt.Sprintf(`
			<div class="machine">
				<div class="machine-header">
					<div>
						<div class="machine-id">%s</div>
						<div style="margin-top: 5px;">%s</div>
					</div>
				</div>
				<div class="stats">
					<div class="stat-item">
						<div class="stat-label">Version</div>
						<div class="stat-value">%s</div>
					</div>
					<div class="stat-item">
						<div class="stat-label">OS/Arch</div>
						<div class="stat-value">%s/%s</div>
					</div>
					<div class="stat-item">
						<div class="stat-label">Total Payloads</div>
						<div class="stat-value">%d</div>
					</div>
					<div class="stat-item">
						<div class="stat-label">Objects</div>
						<div class="stat-value">%s</div>
					</div>
					<div class="stat-item">
						<div class="stat-label">Collections</div>
						<div class="stat-value">%d</div>
					</div>
					<div class="stat-item">
						<div class="stat-label">Modules</div>
						<div class="stat-value">%d</div>
					</div>
				</div>
				%s
				%s
				<div style="margin-top: 10px; font-size: 11px; color: #666;">
					First seen: %s<br>
					Last seen: %s (%s)
				</div>
			</div>`,
			m.MachineID, payloadTypesHtml,
			m.Version, m.OS, m.Arch,
			m.TotalPayloads, formatNumber(m.TotalObjects), m.CollectionsCount, len(modules),
			modulesHtml,
			clientUsageHtml,
			formatTime(m.FirstSeen), formatTime(m.LastSeen), formatRelativeTime(m.LastSeen))
	}
	return html
}

func formatTime(t time.Time) string {
	return t.Format("2006-01-02 15:04:05")
}

func formatRelativeTime(t time.Time) string {
	diff := time.Since(t)
	if diff < time.Minute {
		return fmt.Sprintf("%ds ago", int(diff.Seconds()))
	}
	if diff < time.Hour {
		return fmt.Sprintf("%dm ago", int(diff.Minutes()))
	}
	return fmt.Sprintf("%dh ago", int(diff.Hours()))
}

func formatNumber(n int64) string {
	if n >= 1000000 {
		return fmt.Sprintf("%.1fM", float64(n)/1000000)
	}
	if n >= 1000 {
		return fmt.Sprintf("%.1fK", float64(n)/1000)
	}
	return fmt.Sprintf("%d", n)
}

func joinStrings(strs []string, sep string) string {
	if len(strs) == 0 {
		return ""
	}
	result := strs[0]
	for i := 1; i < len(strs); i++ {
		result += sep + strs[i]
	}
	return result
}

func toLower(s string) string {
	if len(s) == 0 {
		return s
	}
	result := ""
	for _, r := range s {
		if r >= 'A' && r <= 'Z' {
			result += string(r + 32)
		} else {
			result += string(r)
		}
	}
	return result
}
