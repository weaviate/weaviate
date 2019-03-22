package telemetry

import (
	"bytes"
	"encoding/json"
	"net/http"
	"time"

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

// NewReporter creates a new Reporter struct and returns a pointer to it.
func NewReporter(requestsLog *RequestsLog, reportInterval int, reportURL string, telemetryEnabled bool, testing bool) *Reporter {
	return &Reporter{
		log:         requestsLog,
		interval:    reportInterval,
		url:         reportURL,
		enabled:     telemetryEnabled,
		transformer: NewOutputTransformer(testing),
		poster:      NewPoster(reportURL),
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
				// TODO: error logging in etcd
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

func (r *Reporter) triggerCBORFailsafe(extractedLog *map[string]*RequestLog) {
	// TODO fill in failsafe etcd handling
}

// TransformToOutputFormat transforms the logged function calls to a minimized output format to reduce network traffic.
func (r *Reporter) TransformToOutputFormat(logs *map[string]*RequestLog) (*[]byte, error) { // TODO: cover with test
	minimizedLogs := r.transformer.ConvertToMinimizedJSON(logs)

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

// ConvertToMinimizedJSON converts the request logs to minimized JSON
func (o *OutputTransformer) ConvertToMinimizedJSON(logs *map[string]*RequestLog) *string {
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

// EncodeAsCBOR encodes logs in CBOR format and returns them as a byte array (format to base 16 to get the 'traditional' cbor format).
func (o *OutputTransformer) EncodeAsCBOR(minimizedJSON *string) (*[]byte, error) {
	encoded := make([]byte, 0, 64)
	cborHandle := new(codec.CborHandle)
	if o.testing {
		cborHandle.Canonical = true
	}

	encoder := codec.NewEncoderBytes(&encoded, cborHandle)
	err := encoder.Encode(minimizedJSON)

	if err != nil {
		return nil, err
	}

	return &encoded, nil
}

// NewPoster creates a new poster struct, which is responsible for sending logs to the specified endpoint.
func NewPoster(url string) *Poster {
	return &Poster{url: url}
}

// Poster is a class responsible for sending the converted log to the logging endpoint. If the endpoint is unreachable then the logs are stored in the etcd store.
type Poster struct {
	url string
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

}
