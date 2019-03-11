package telemetry

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

	"github.com/creativesoftwarefdn/weaviate/telemetry"

	"github.com/2tvenom/cbor"
)

const Name string = "n"
const Type string = "t"
const Identifier string = "i"
const Amount string = "a"
const When string = "w"

// TODO:
// make new outputTransformer
// minimize logs
// cborize logs
// send + retry
// failsafe

func NewReporter(requestsLog *RequestsLog, reportInterval int, reportURL string, telemetryEnabled bool) *Reporter {
	return &Reporter{
		log:         requestsLog,
		interval:    reportInterval,
		url:         reportURL,
		enabled:     telemetryEnabled,
		transformer: NewOutputTransformer(),
		poster:      NewPoster(reportURL),
	}
}

type Reporter struct {
	log         *RequestsLog
	interval    int
	url         string
	enabled     bool
	transformer *outputTransformer
	poster      *poster
}

// Post logged function calls in CBOR format to the provided url every <provided interval> seconds.
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
				// TODO: error logging in etcd
			}

		}
	}
}

// Add the current timestamp to the logged requests types
func (r *Reporter) AddTimeStamps(extractedLog *map[string]*RequestLog) {
	timestamp := time.Now().Unix()

	for _, log := range *extractedLog {
		log.When = timestamp
	}
}

func (r *Reporter) triggerCBORFailsafe(requests *map[string]*telemetry.RequestLog) {
	// TODO fill in failsafe etcd handling
}

// TODO: cover with test
// Transform the logged function calls to a minimized output format to reduce network traffic.
func (r *Reporter) TransformToOutputFormat(logs *map[string]*RequestLog) ([]byte, error) {
	minimizedLogs := r.transformer.ConvertToMinimizedJSON(logs)

	cborLogs, err := r.transformer.EncodeAsCBOR(minimizedLogs)
	if err != nil {
		return nil, err
	}

	return cborLogs, nil
}

type outputTransformer struct {
}

func NewOutputTransformer() *outputTransformer {
	return &outputTransformer{}
}

// Convert the request logs to minimized JSON
func (o *outputTransformer) ConvertToMinimizedJSON(logs *map[string]*RequestLog) *string {
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

	rawMinimizedJSON, _ := json.Marshal(minimizedLogs)
	minimizedJSON := string(rawMinimizedJSON)
	return &minimizedJSON
}

// Encode the logs in CBOR format
func (o *outputTransformer) EncodeAsCBOR(minimizedJSON *string) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := cbor.NewEncoder(&buffer)
	ok, err := encoder.Marshal(minimizedJSON)

	if !ok {
		return nil, err
	} else {
		return buffer.Bytes(), nil
	}
}

func NewPoster(url string) *poster {
	return &poster{url: url}
}

// The class responsible for sending the converted log to the desired location.
// Tries to send the log to a REST endpoint. If the endpoint is unreachable then the logs are stored in the etcd store.
type poster struct {
	url string
}

// Send the logs to a previously determined REST endpoint
func (p *poster) ReportLoggedCalls(encoded []byte) {
	req, err := http.NewRequest("POST", p.url, bytes.NewReader(encoded))
	//req.Header.Set("X-Custom-Header", "myvalue")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil || resp.Status != "200" {
		p.triggerPOSTFailsafe(encoded)
	}
	defer resp.Body.Close()
}

// Should the REST endpoint be unreachable then the log is stored in the etcd key item store
func (p *poster) triggerPOSTFailsafe(encoded []byte) {

}
