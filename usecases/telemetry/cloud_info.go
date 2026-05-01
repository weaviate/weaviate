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
	"regexp"
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

type cloudInfoHelper struct {
	provider cloudInfoProvider
	logger   logrus.FieldLogger
}

func newCloudInfoHelper(logger logrus.FieldLogger) *cloudInfoHelper {
	aws := newAWSCloudInfo("http://169.254.169.254")
	if aws.isDetected() {
		logTelemetryInfo(logger)
		return &cloudInfoHelper{logger: logger, provider: aws}
	}

	gcp := newGCPCloudInfo("http://metadata.google.internal/computeMetadata/v1")
	if gcp.isDetected() {
		logTelemetryInfo(logger)
		return &cloudInfoHelper{logger: logger, provider: gcp}
	}

	azure := newAzureCloudInfo("http://169.254.169.254", "2021-02-01")
	if azure.isDetected() {
		logTelemetryInfo(logger)
		return &cloudInfoHelper{logger: logger, provider: azure}
	}

	return &cloudInfoHelper{logger: logger}
}

func (c *cloudInfoHelper) getCloudInfo() *cloudInfo {
	if c.provider != nil {
		return c.provider.getCloudInfo()
	}
	return nil
}

type awsCloudInfo struct {
	metadataURL, tokenURL, documentURL string
}

func newAWSCloudInfo(baseURL string) *awsCloudInfo {
	return &awsCloudInfo{
		metadataURL: fmt.Sprintf("%s/latest/meta-data/", baseURL),
		tokenURL:    fmt.Sprintf("%s/latest/api/token", baseURL),
		documentURL: fmt.Sprintf("%s/latest/dynamic/instance-identity/document", baseURL),
	}
}

func (c *awsCloudInfo) isDetected() bool {
	_, awsStatus, _ := sendRequest(c.metadataURL, nil, "GET")
	return awsStatus == 200 || awsStatus == 401
}

func (c *awsCloudInfo) getCloudInfo() *cloudInfo {
	headers := map[string]string{"X-aws-ec2-metadata-token-ttl-seconds": "21600"}
	token, status, _ := sendRequest(c.tokenURL, headers, "PUT")

	headers = nil
	if status == 200 {
		headers = map[string]string{"X-aws-ec2-metadata-token": token}
	}

	accountID := ""
	doc, _, _ := sendRequest(c.documentURL, headers, "GET")
	if re, err := regexp.Compile(`"accountId"\s*:\s*"([^"]+)"`); err == nil {
		if match := re.FindStringSubmatch(doc); len(match) > 1 {
			accountID = match[1]
		}
	}

	return &cloudInfo{
		cloudProvider: "AWS",
		uniqueID:      accountID,
	}
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

func sendRequest(url string, headers map[string]string, method string) (string, int, error) {
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return "", 0, fmt.Errorf("create new request: %w", err)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{Timeout: 1 * time.Second}
	resp, err := client.Do(req)
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
