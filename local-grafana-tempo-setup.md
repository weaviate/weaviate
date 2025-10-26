# Running Grafana Locally with Tempo Pod Port Forwarding

## Step 1: Port Forward to Tempo Pod

First, find your Tempo pod and port forward to it:

```bash
# Find the Tempo pod
kubectl get pods -l app=tempo

# Port forward to Tempo (replace tempo-pod with actual pod name)
kubectl port-forward tempo-pod 3200:3200 4317:4317 4318:4318
```

## Step 2: Run Grafana Locally

### Option A: Using Docker Compose (Recommended)

Create a `docker-compose-local-grafana.yml`:

```yaml
version: '3.8'
services:
  grafana:
    image: grafana/grafana-oss:latest
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - ./local-grafana-config/datasources:/etc/grafana/provisioning/datasources
      - ./local-grafana-config/dashboards:/etc/grafana/provisioning/dashboards
      - ./local-grafana-config/dashboard-files:/var/lib/grafana/dashboards
```

### Option B: Using Docker Run

```bash
docker run -d \
  --name local-grafana \
  -p 3000:3000 \
  -e GF_SECURITY_ADMIN_PASSWORD=admin \
  -v $(pwd)/local-grafana-config/datasources:/etc/grafana/provisioning/datasources \
  -v $(pwd)/local-grafana-config/dashboards:/etc/grafana/provisioning/dashboards \
  -v $(pwd)/local-grafana-config/dashboard-files:/var/lib/grafana/dashboards \
  grafana/grafana-oss:latest
```

## Step 3: Create Local Grafana Configuration

Create the directory structure:
```bash
mkdir -p local-grafana-config/{datasources,dashboards,dashboard-files}
```

### Tempo Datasource Configuration

Create `local-grafana-config/datasources/tempo.yml`:

```yaml
apiVersion: 1
datasources:
  - name: Tempo
    type: tempo
    access: proxy
    orgId: 1
    uid: tempo
    url: http://host.docker.internal:3200
    basicAuth: false
    isDefault: false
    version: 1
    editable: true
    jsonData:
      httpMethod: GET
      serviceMap:
        datasourceUid: prometheus
```

### Prometheus Datasource Configuration

Create `local-grafana-config/datasources/prometheus.yml`:

```yaml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    access: proxy
    orgId: 1
    uid: prometheus
    url: http://host.docker.internal:9090
    basicAuth: false
    isDefault: true
    version: 1
    editable: false
    jsonData:
      timeInterval: "5s"
```

## Step 4: Start Everything

1. **Start port forwarding** (in terminal 1):
   ```bash
   kubectl port-forward tempo-pod 3200:3200 4317:4317 4318:4318
   ```

2. **Start Grafana** (in terminal 2):
   ```bash
   # Using docker-compose
   docker-compose -f docker-compose-local-grafana.yml up
   
   # Or using docker run
   docker run -d --name local-grafana -p 3000:3000 \
     -e GF_SECURITY_ADMIN_PASSWORD=admin \
     -v $(pwd)/local-grafana-config/datasources:/etc/grafana/provisioning/datasources \
     grafana/grafana-oss:latest
   ```

3. **Access Grafana**:
   - Open http://localhost:3000
   - Login: admin/admin

## Step 5: Verify Tempo Connection

1. Go to **Configuration > Data Sources**
2. Click on **Tempo** datasource
3. Click **Test** - should show "Data source is working"
4. Go to **Explore** and select **Tempo** from the dropdown
5. You should see trace data if Weaviate is sending traces

## Troubleshooting

### If Tempo connection fails:

1. **Check port forwarding**:
   ```bash
   # Test Tempo directly
   curl http://localhost:3200/status
   ```

2. **Check if Weaviate is sending traces**:
   ```bash
   # Check Weaviate pod logs
   kubectl logs <weaviate-pod-name> | grep -i otel
   ```

3. **Verify Tempo is receiving traces**:
   ```bash
   # Check Tempo pod logs
   kubectl logs tempo-pod | grep -i trace
   ```

### Alternative: Using host networking

If `host.docker.internal` doesn't work, use host networking:

```bash
docker run -d \
  --name local-grafana \
  --network host \
  -e GF_SECURITY_ADMIN_PASSWORD=admin \
  -v $(pwd)/local-grafana-config/datasources:/etc/grafana/provisioning/datasources \
  grafana/grafana-oss:latest
```

Then update the datasource URLs to use `localhost` instead of `host.docker.internal`.

## Step 6: View Traces

1. In Grafana, go to **Explore**
2. Select **Tempo** as the data source
3. Use the search to find traces:
   - Service: `weaviate`
   - Look for recent traces
4. Click on a trace to see the detailed trace view

## Optional: Add Service Map

To see the service map, you'll also need to port forward to Prometheus:

```bash
# Port forward to Prometheus (if running in k8s)
kubectl port-forward service/prometheus-service 9090:9090
```

This will enable the service map feature in Grafana to visualize the relationships between services.
