package httpv2

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/pkg/errors"
	utils "github.com/weaviate/weaviate/modules/text2vec-kserve/clients/utils"
	"github.com/weaviate/weaviate/modules/text2vec-kserve/ent"
)

func (c *HttpV2Client) Validate(ctx context.Context, config ent.ModuleConfig) error {
	if config.Protocol != ent.KSERVE_HTTP_V2 {
		return fmt.Errorf("`vectorize` was called with wrong protocol %v", config.Protocol)
	}
	var client *http.Client
	serviceUrl := config.Url
	cachedClient, ok := c.connection(serviceUrl)

	if !ok {
		connArgs, err := utils.ToHttpConnectionArgs(config.ConnectionArgs)
		if err != nil {
			return errors.Wrap(err, "invalid http connection args")
		}
		client, err = c.createConnection(ctx, serviceUrl, *connArgs)

		if err != nil {
			return errors.Wrap(err, "couldn't create a http client")
		}
	} else {
		client = cachedClient
	}

	metadata, err := c.modelMetadata(ctx, *client, serviceUrl, config.Model, config.Version)
	if err != nil {
		return err
	}

	return c.validate(ctx, metadata, config)
}

func (c *HttpV2Client) modelMetadata(ctx context.Context, conn http.Client, url string, modelName string, modelVersion string) (*modelMetadataResponse, error) {
	fullUrl := appendMetadataEndpoint(url, modelName, modelVersion)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullUrl, nil)
	if err != nil {
		return nil, errors.Wrap(err, "create metadata request")
	}

	resp, err := conn.Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "metadata request")
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 || resp.StatusCode < 400 {
		return parseMetadataResponse(resp.Body)
	} else if resp.StatusCode >= 400 || resp.StatusCode < 500 {
		return nil, parseMetadataError(resp.Request.Body)
	} else {
		return nil, fmt.Errorf("received 5xx status code from %v", url)
	}
}

func parseMetadataResponse(r io.ReadCloser) (*modelMetadataResponse, error) {
	var metadata interface{} = &modelMetadataResponse{}
	if err := utils.DecodeJson(r, &metadata); err != nil {
		return nil, errors.Wrap(err, "parse metadata response")
	}
	return metadata.(*modelMetadataResponse), nil
}

func parseMetadataError(r io.ReadCloser) error {
	var errorResponse interface{} = &modelMetadataError{}
	if err := utils.DecodeJson(r, &errorResponse); err != nil {
		return errors.Wrap(err, "parse metadata error")
	}
	return fmt.Errorf("request for metadata failed %v", errorResponse)
}

func appendMetadataEndpoint(u string, modelName string, modelVersion string) string {
	nameEscaped := url.PathEscape(modelName)

	if versionEscaped := url.PathEscape(modelVersion); versionEscaped != "" {
		return fmt.Sprintf("%v/v2/models/%v/versions/%v", u, nameEscaped, versionEscaped)
	}
	return fmt.Sprintf("%v/v2/models/%v", u, nameEscaped)
}

func (c *HttpV2Client) validate(ctx context.Context, metadata *modelMetadataResponse, config ent.ModuleConfig) error {
	inputTensor := findTensor(metadata.Inputs, config.Input)
	if inputTensor == nil {
		return fmt.Errorf("no input %v for model %v", config.Input, config.Model)
	}

	if !utils.TensorShapeEqual(inputTensor.Shape, []int32{-1, 1}) {
		return fmt.Errorf("incorrect shape %v on input %v, expected [-1 %v]", inputTensor.Shape, inputTensor.Name, 1)
	}

	if inputTensor.Datatype != "BYTES" {
		return fmt.Errorf("incorrect datatype %v on input tensor %v, expected BYTES", inputTensor.Datatype, inputTensor.Name)
	}

	outputTensor := findTensor(metadata.Outputs, config.Output)
	if outputTensor == nil {
		return fmt.Errorf("no output %v for model %v", config.Output, config.Model)
	}

	if !utils.TensorShapeEqual(outputTensor.Shape, []int32{-1, config.EmbeddingDims}) {
		return fmt.Errorf("incorrect shape %v on output %v, expected [-1 %v]", inputTensor.Shape, inputTensor.Name, config.EmbeddingDims)
	}

	if outputTensor.Datatype != "FP32" {
		return fmt.Errorf("incorrect datatype %v on output tensor %v, expected FP32", outputTensor.Datatype, outputTensor.Name)
	}

	return nil
}

func findTensor(tensors []tensorSpec, tensorName string) *tensorSpec {
	for _, i := range tensors {
		if i.Name == tensorName {
			return &i
		}
	}
	return nil
}
