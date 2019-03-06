package telemetry

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/2tvenom/cbor"
)

const Name string = "n"
const Type string = "t"
const Identifier string = "i"
const Amount string = "a"
const When string = "w"

func NewReporter(requestsLog *RequestsLog, reportInterval int, reportURL string, telemetryEnabled bool) *Reporter {
	return &Reporter{
		log:      requestsLog,
		interval: reportInterval,
		url:      reportURL,
		enabled:  telemetryEnabled,
	}
}

type Reporter struct {
	log      *RequestsLog
	interval int
	url      string
	enabled  bool
}

// Post logged function calls in CBOR format to the provided url every <provided interval> seconds.
// Contains a failsafe mechanism in the case the url is unreachable.
func (r *Reporter) Start() {
	if r.enabled {
		for {
			time.Sleep(time.Duration(r.interval) * time.Second)
			extractedLog := r.log.ExtractLoggedRequests()
			r.AddTimeStamps(extractedLog)
			/*transformedLog, err := */ r.TransformToOutputFormat(extractedLog)

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

// TODO: cover with test
// Transform the logged function calls to a minimized output format to reduce network traffic.
func (r *Reporter) TransformToOutputFormat(logs *map[string]*RequestLog) ([]uint8, error) {
	transformer := NewOutputTransformer()

	minimizedLogs := transformer.ConvertToMinimizedJSON(logs)

	cborLogs, err := transformer.EncodeAsCBOR(minimizedLogs)
	if err != nil {
		return nil, err
	}
	// TODO:
	// make new outputTransformer
	// minimize logs
	// cborize logs
	// send + retry
	// failsafe

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
func (o *outputTransformer) EncodeAsCBOR(minimizedJSON *string) ([]uint8, error) {
	var buffer bytes.Buffer
	encoder := cbor.NewEncoder(&buffer)
	ok, err := encoder.Marshal(minimizedJSON)

	encoded := buffer.Bytes()

	if !ok { // TODO: handle error, pending issue #737
		return nil, err
	} else {
		return encoded, nil
	}
}
