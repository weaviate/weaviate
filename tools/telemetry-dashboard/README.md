# Weaviate Telemetry Dashboard

A local web dashboard for receiving and visualizing telemetry data from Weaviate instances.

## Features

- **Real-time telemetry reception**: Accepts telemetry POST requests from Weaviate instances
- **Live dashboard**: Web-based UI showing:
  - Recent telemetry payloads with full details
  - Machine statistics aggregated by machine ID
  - Client usage tracking (Python, Java, TypeScript, Go, C#)
  - Module usage information
  - Object and collection counts
- **Auto-refresh**: Dashboard updates every 2 seconds automatically
- **Multiple instances**: Tracks multiple Weaviate instances simultaneously

## Usage

### Start the Dashboard

```bash
go run tools/telemetry-dashboard/main.go
```

The dashboard will start on `http://localhost:8080`

### Configure Weaviate to Send Telemetry

To configure a Weaviate instance to send telemetry to the local dashboard, you need to set the telemetry consumer URL. The default telemetry endpoint is base64-encoded. You can set a custom endpoint using environment variables or configuration.

**Option 1: Using Environment Variable (if supported)**

Set the telemetry consumer URL to point to your local dashboard:
```
WEAVIATE_TELEMETRY_URL=http://localhost:8080/weaviate-telemetry
```

**Option 2: Modify Weaviate Configuration**

The telemetry consumer URL is base64-encoded in the code. You would need to modify the `defaultConsumer` constant in `usecases/telemetry/telemetry.go` or use a configuration option if available.

To encode your URL:
```bash
echo -n "http://localhost:8080/weaviate-telemetry" | base64
```

Then use this encoded value in your Weaviate configuration.

## Dashboard Views

### Recent Payloads
Shows the most recent 100 telemetry payloads received, including:
- Payload type (INIT, UPDATE, TERMINATE)
- Machine ID
- Weaviate version
- OS and architecture
- Object counts
- Collection counts
- Client usage statistics
- Used modules

### Machines
Aggregated view per machine showing:
- Total payloads received
- Payload type breakdown
- Total objects stored
- Collection count
- All modules used
- Total client usage across all payloads
- First seen / Last seen timestamps

## API Endpoints

- `GET /` - Main dashboard (HTML)
- `POST /weaviate-telemetry` - Telemetry endpoint (receives Weaviate telemetry)
- `GET /api/data` - JSON API for dashboard data (used for auto-refresh)

## Example Telemetry Payload

The dashboard expects telemetry payloads in this format:

```json
{
  "machineId": "uuid-here",
  "type": "UPDATE",
  "version": "1.23.0",
  "objs": 1000,
  "os": "linux",
  "arch": "amd64",
  "usedModules": ["text2vec-openai", "generative-openai"],
  "collectionsCount": 5,
  "clientUsage": {
    "python": 150,
    "java": 75,
    "typescript": 50
  }
}
```

## Development

The dashboard is a standalone Go application that can be easily extended with additional features like:
- Historical data persistence
- Export functionality
- Filtering and search
- Charts and graphs
- Alerting
