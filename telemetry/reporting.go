package telemetry

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/ugorji/go/codec"
)

// Name contains the minimized string value of the Name field.
const Name string = "n"

// Type contains the minimized string value of the Type field.
const Type string = "t"

// Identifier contains the minimized string value of the Identifier field.
const Identifier string = "i"

// Amount contains the minimized string value of the Amount field.
const Amount string = "a"

// When contains the minimized string value of the When field.
const When string = "w"

// Local interface for an etcd client.
type etcdClient interface {
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
}

// NewReporter creates a new Reporter struct and returns a pointer to it.
func NewReporter(requestsLog *RequestsLog, reportInterval int, reportURL string, telemetryEnabled bool, testing bool, client etcdClient) *Reporter {
	return &Reporter{
		log:         requestsLog,
		interval:    reportInterval,
		url:         reportURL,
		enabled:     telemetryEnabled,
		transformer: NewOutputTransformer(testing),
		poster:      NewPoster(reportURL, client),
		client:      client,
	}
}

// Reporter is the struct used to report the logged function calls, either to an endpoint or to the etcd store.
type Reporter struct {
	log         *RequestsLog
	interval    int
	url         string
	enabled     bool
	transformer *OutputTransformer
	poster      *Poster
	client      etcdClient
}

// Start posts logged function calls in CBOR format to the provided url every <provided interval> seconds.
// Contains a failsafe mechanism in the case the url is unreachable.
func (r *Reporter) Start() {
	if r.enabled {
		for {
			time.Sleep(time.Duration(r.interval) * time.Second)
			extractedLog := r.log.ExtractLoggedRequests()
			r.AddTimeStamps(extractedLog)
			transformedLog, err := r.TransformToOutputFormat(extractedLog)
			if err == nil {
				r.poster.ReportLoggedCalls(transformedLog)
			} else {
				r.triggerCBORFailsafe(extractedLog)
			}
		}
	}
}

// AddTimeStamps adds the current timestamp to the logged request types.
func (r *Reporter) AddTimeStamps(extractedLog *map[string]*RequestLog) {
	timestamp := time.Now().Unix()

	for _, log := range *extractedLog {
		log.When = timestamp
	}
}

// triggerPOSTFailsafe stores the raw log in the etcd key item store if CBOR conversion fails.
func (r *Reporter) triggerCBORFailsafe(extractedLog *map[string]*RequestLog) {
	currentTime := time.Now()
	key := fmt.Sprintf("%s %d-%02d-%02d %02d:%02d:%02d", ReportCBORFail, currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour(), currentTime.Minute(), currentTime.Second())
	value := fmt.Sprintf("%v", *extractedLog)

	_, err := r.client.Put(nil, key, value)
	if err != nil {
		fmt.Errorf("could not send raw log to etcd: %s", err)
	}
}

// TransformToOutputFormat transforms the logged function calls to a minimized output format to reduce network traffic.
func (r *Reporter) TransformToOutputFormat(logs *map[string]*RequestLog) (*[]byte, error) { // TODO: cover with test
	minimizedLogs := r.transformer.Minimize(logs)

	cborLogs, err := r.transformer.EncodeAsCBOR(minimizedLogs)
	if err != nil {
		return nil, err
	}

	return cborLogs, nil
}

// OutputTransformer contains methods that transform the function call logs to an outputtable format.
type OutputTransformer struct {
	testing bool
}

// NewOutputTransformer creates a new OutputTransformer and returns a pointer to it.
func NewOutputTransformer(testing bool) *OutputTransformer {
	return &OutputTransformer{testing}
}

// Minimize converts the request logs to a minimized format
func (o *OutputTransformer) Minimize(logs *map[string]*RequestLog) *[]map[string]interface{} {
	minimizedLogs := make([]map[string]interface{}, len(*logs))

	iterations := 0
	for _, loggedRequest := range *logs {
		miniLog := make(map[string]interface{})

		miniLog[Name] = loggedRequest.Name
		miniLog[Type] = loggedRequest.Type
		miniLog[Identifier] = loggedRequest.Identifier
		miniLog[Amount] = loggedRequest.Amount
		miniLog[When] = loggedRequest.When

		minimizedLogs[iterations] = miniLog
		iterations++
	}

	return &minimizedLogs
}

// EncodeAsCBOR encodes logs in CBOR format and returns them as a byte array (format to base 16 to get the 'traditional' cbor format).
func (o *OutputTransformer) EncodeAsCBOR(minimizedLogs *[]map[string]interface{}) (*[]byte, error) {
	encoded := make([]byte, 0, 64)
	cborHandle := new(codec.CborHandle)
	if o.testing {
		cborHandle.Canonical = true
	}

	encoder := codec.NewEncoderBytes(&encoded, cborHandle)
	err := encoder.Encode(minimizedLogs)

	if err != nil {
		return nil, err
	}

	return &encoded, nil
}

// NewPoster creates a new poster struct, which is responsible for sending logs to the specified endpoint.
func NewPoster(url string, client etcdClient) *Poster {
	return &Poster{
		url:    url,
		client: client,
	}
}

// Poster is a class responsible for sending the converted log to the logging endpoint. If the endpoint is unreachable then the logs are stored in the etcd store.
type Poster struct {
	url    string
	client etcdClient
}

// ReportLoggedCalls sends the logs to a previously determined REST endpoint.
func (p *Poster) ReportLoggedCalls(encoded *[]byte) {
	req, err := http.NewRequest("POST", p.url, bytes.NewReader(*encoded))
	req.Header.Set("Content-Type", "application/cbor")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil || resp.Status != "200" {
		p.triggerPOSTFailsafe(encoded)
	} else {
		defer resp.Body.Close()
	}
}

// triggerPOSTFailsafe stores the log in the etcd key item store if the POST fails.
func (p *Poster) triggerPOSTFailsafe(encoded *[]byte) {
	currentTime := time.Now()
	key := fmt.Sprintf("%s %d-%02d-%02d %02d:%02d:%02d", ReportPostFail, currentTime.Year(), currentTime.Month(), currentTime.Day(), currentTime.Hour(), currentTime.Minute(), currentTime.Second())

	_, err := p.client.Put(nil, key, string(encoded))
	if err != nil {
		fmt.Errorf("could not send encoded log to etcd: %s", err)
	}
}
