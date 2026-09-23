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

package telemetry

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type cloudInfo struct {
	cloudProvider string
	uniqueID      string
}

type cloudInfoProvider interface {
	getCloudInfo() *cloudInfo
}

// cloudInfoHelper detects the cloud provider lazily: newCloudInfoHelper does
// no network I/O, so building a Telemeter never blocks server startup. The
// first provider a caller finds is cached; while none is found, detection is
// retried on every call. getCloudInfo is only ever called from buildPayload,
// which only ever runs inside the telemetry goroutine (Start and its ticker
// loop), so retries never land on the synchronous startup path either.
type cloudInfoHelper struct {
	logger  logrus.FieldLogger
	enabled bool
	// detect finds a cloud provider, or returns nil if none is detected yet.
	// Set by newCloudInfoHelper to detectRealProvider; overridable in tests.
	// A cloudInfoHelper built as a bare struct literal (every existing test
	// that sets `provider` directly) leaves this nil, which is fine because
	// getCloudInfo only calls it when provider is still nil.
	detect func() cloudInfoProvider

	mu       sync.Mutex
	provider cloudInfoProvider
}

func newCloudInfoHelper(logger logrus.FieldLogger, telemetryEnabled bool) *cloudInfoHelper {
	c := &cloudInfoHelper{logger: logger, enabled: telemetryEnabled}
	c.detect = c.detectRealProvider
	return c
}

func (c *cloudInfoHelper) getCloudInfo() *cloudInfo {
	provider := c.cachedProvider()
	if provider == nil {
		provider = c.detectAndCache()
		if provider == nil {
			return nil
		}
	}
	return provider.getCloudInfo()
}

func (c *cloudInfoHelper) cachedProvider() cloudInfoProvider {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.provider
}

// detectAndCache runs c.detect and caches the first non-nil result. A test
// that constructs a cloudInfoHelper directly with a pre-set provider
// (bypassing newCloudInfoHelper) never reaches here.
func (c *cloudInfoHelper) detectAndCache() cloudInfoProvider {
	if !c.enabled || c.detect == nil {
		return nil
	}

	provider := c.detect()
	if provider == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.provider = provider
	return provider
}

// detectRealProvider tries each cloud provider's real metadata endpoint in
// turn. It is c's default detect function; tests substitute their own.
func (c *cloudInfoHelper) detectRealProvider() cloudInfoProvider {
	aws := newAWSCloudInfo(awsIMDSIPv4BaseURL, awsIMDSIPv6BaseURL, os.Getenv("ECS_CONTAINER_METADATA_URI_V4"), c.logger)
	gcp := newGCPCloudInfo(gcpMetadataBaseURL)
	azure := newAzureCloudInfo(azureMetadataBaseURL, azureAPIVersion)
	switch {
	case aws.isDetected():
		return aws
	case gcp.isDetected():
		return gcp
	case azure.isDetected():
		return azure
	default:
		return nil
	}
}

const (
	awsIMDSIPv4BaseURL = "http://169.254.169.254"
	// awsIMDSIPv6BaseURL is AWS's link-local IMDS endpoint for IPv6-only
	// networking, tried when the IPv4 endpoint is unreachable.
	awsIMDSIPv6BaseURL   = "http://[fd00:ec2::254]"
	gcpMetadataBaseURL   = "http://metadata.google.internal/computeMetadata/v1"
	azureMetadataBaseURL = "http://169.254.169.254"
	azureAPIVersion      = "2021-02-01"
)

type awsCloudInfo struct {
	metadataURL, tokenURL, documentURL             string
	ipv6MetadataURL, ipv6TokenURL, ipv6DocumentURL string
	// ecsTaskMetadataURL is $ECS_CONTAINER_METADATA_URI_V4/task, empty
	// outside ECS/Fargate. It never reads role credentials or calls STS;
	// the account id comes only from the task metadata endpoint's own
	// TaskARN field.
	ecsTaskMetadataURL string
	logger             logrus.FieldLogger
	warnOnce           sync.Once
}

func newAWSCloudInfo(baseURL, ipv6BaseURL, ecsMetadataURI string, logger logrus.FieldLogger) *awsCloudInfo {
	c := &awsCloudInfo{
		metadataURL: fmt.Sprintf("%s/latest/meta-data/", baseURL),
		tokenURL:    fmt.Sprintf("%s/latest/api/token", baseURL),
		documentURL: fmt.Sprintf("%s/latest/dynamic/instance-identity/document", baseURL),
		logger:      logger,
	}
	if ipv6BaseURL != "" {
		c.ipv6MetadataURL = fmt.Sprintf("%s/latest/meta-data/", ipv6BaseURL)
		c.ipv6TokenURL = fmt.Sprintf("%s/latest/api/token", ipv6BaseURL)
		c.ipv6DocumentURL = fmt.Sprintf("%s/latest/dynamic/instance-identity/document", ipv6BaseURL)
	}
	if ecsMetadataURI != "" {
		c.ecsTaskMetadataURL = ecsMetadataURI + "/task"
	}
	return c
}

func (c *awsCloudInfo) isDetected() bool {
	if c.imdsAnswers(c.metadataURL) {
		return true
	}
	if c.imdsAnswers(c.ipv6MetadataURL) {
		return true
	}
	// The ECS agent injects this env var only inside an ECS/Fargate task, so
	// its presence alone is a reliable detection signal without a network call.
	return c.ecsTaskMetadataURL != ""
}

func (c *awsCloudInfo) imdsAnswers(metadataURL string) bool {
	if metadataURL == "" {
		return false
	}
	_, status, _ := sendRequest(metadataURL, nil, "GET")
	return status == 200 || status == 401
}

func (c *awsCloudInfo) getCloudInfo() *cloudInfo {
	accountID := c.readIMDSAccountID(c.tokenURL, c.documentURL)
	if accountID == "" {
		accountID = c.readIMDSAccountID(c.ipv6TokenURL, c.ipv6DocumentURL)
	}
	if accountID == "" {
		accountID = c.readECSAccountID()
	}
	if accountID == "" {
		c.warnOnce.Do(func() {
			c.logger.WithField("action", "telemetry_cloud_info").
				Warn("AWS detected but no account id could be read from the IPv4 IMDS, IPv6 IMDS or ECS task metadata endpoints")
		})
	}
	return &cloudInfo{cloudProvider: "AWS", uniqueID: accountID}
}

// readIMDSAccountID fetches an IMDSv2 token, then the identity document,
// returning "" if either URL is unset. When the token request fails, the
// document GET is still tried without a token (the IMDSv1 shape), since
// some environments allow that even where IMDSv2 is available.
func (c *awsCloudInfo) readIMDSAccountID(tokenURL, documentURL string) string {
	if tokenURL == "" || documentURL == "" {
		return ""
	}
	headers := map[string]string{"X-aws-ec2-metadata-token-ttl-seconds": "21600"}
	token, status, _ := sendRequest(tokenURL, headers, "PUT")

	headers = nil
	if status == 200 {
		headers = map[string]string{"X-aws-ec2-metadata-token": token}
	}

	doc, _, _ := sendRequest(documentURL, headers, "GET")
	return extractAWSAccountID(doc)
}

var awsAccountIDPattern = regexp.MustCompile(`"accountId"\s*:\s*"([^"]+)"`)

func extractAWSAccountID(doc string) string {
	if match := awsAccountIDPattern.FindStringSubmatch(doc); len(match) > 1 {
		return match[1]
	}
	return ""
}

// readECSAccountID reads the account id from the task's own ARN via the ECS
// task metadata endpoint (metadata only - no role credentials, no STS call).
func (c *awsCloudInfo) readECSAccountID() string {
	if c.ecsTaskMetadataURL == "" {
		return ""
	}
	body, status, _ := sendRequest(c.ecsTaskMetadataURL, nil, "GET")
	if status != 200 {
		return ""
	}
	var task struct {
		TaskARN string `json:"TaskARN"`
	}
	if err := json.Unmarshal([]byte(body), &task); err != nil {
		return ""
	}
	return accountIDFromARN(task.TaskARN)
}

// accountIDFromARN extracts the fixed fifth field of an ARN
// (arn:partition:service:region:account-id:resource).
func accountIDFromARN(arn string) string {
	parts := strings.SplitN(arn, ":", 6)
	if len(parts) < 5 {
		return ""
	}
	return parts[4]
}

type gcpCloudInfo struct {
	instanceURL, projectIDURL string
}

func newGCPCloudInfo(baseURL string) *gcpCloudInfo {
	return &gcpCloudInfo{
		instanceURL:  fmt.Sprintf("%s/instance/", baseURL),
		projectIDURL: fmt.Sprintf("%s/project/project-id", baseURL),
	}
}

func (c *gcpCloudInfo) isDetected() bool {
	_, gcpStatus, _ := sendRequest(c.instanceURL, map[string]string{"Metadata-Flavor": "Google"}, "GET")
	return gcpStatus == 200
}

func (c *gcpCloudInfo) getCloudInfo() *cloudInfo {
	headers := map[string]string{"Metadata-Flavor": "Google"}
	projectID, _, _ := sendRequest(c.projectIDURL, headers, "GET")

	return &cloudInfo{
		cloudProvider: "GCP",
		uniqueID:      projectID,
	}
}

type azureCloudInfo struct {
	instanceURL, computeURL string
}

func newAzureCloudInfo(baseURL, apiVersion string) *azureCloudInfo {
	return &azureCloudInfo{
		instanceURL: fmt.Sprintf("%s/metadata/instance?api-version=%s", baseURL, apiVersion),
		computeURL:  fmt.Sprintf("%s/metadata/instance/compute?api-version=%s", baseURL, apiVersion),
	}
}

func (c *azureCloudInfo) isDetected() bool {
	_, azureStatus, _ := sendRequest(c.instanceURL, map[string]string{"Metadata": "true"}, "GET")
	return azureStatus == 200
}

func (c *azureCloudInfo) getCloudInfo() *cloudInfo {
	headers := map[string]string{"Metadata": "true"}
	rawJSON, _, _ := sendRequest(c.computeURL, headers, "GET")
	var data map[string]any
	json.Unmarshal([]byte(rawJSON), &data)

	var subcriptionID string
	if len(data) > 0 {
		if val, ok := data["subscriptionId"]; ok {
			if subID, ok := val.(string); ok {
				subcriptionID = subID
			}
		}
	}

	return &cloudInfo{
		cloudProvider: "Azure",
		uniqueID:      subcriptionID,
	}
}

func logTelemetryInfo(logger logrus.FieldLogger) {
	logger.Info("If you’re running Weaviate on a cloud provider, Weaviate might try to collect basic metadata from the instance. This allows us to understand large-scale deployments better")
	logger.Info("To opt-out at any time, update your system configuration: `DISABLE_TELEMETRY=true`")
	logger.Info("Learn more and view our privacy policy: https://weaviate.io/privacy")
	logger.Info("Learn more about our telemetrics: https://docs.weaviate.io/deploy/configuration/telemetry")
}

// metadataHTTPClient never honours HTTP_PROXY/HTTPS_PROXY/NO_PROXY: an
// operator's egress proxy config must not redirect an in-VM loopback probe
// bound for 169.254.169.254 or metadata.google.internal.
var metadataHTTPClient = &http.Client{
	Timeout:   1 * time.Second,
	Transport: &http.Transport{Proxy: nil},
}

func sendRequest(url string, headers map[string]string, method string) (string, int, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return "", 0, fmt.Errorf("create new request: %w", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := metadataHTTPClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 0, fmt.Errorf("read response: %w", err)
	}
	return string(body), resp.StatusCode, nil
}
