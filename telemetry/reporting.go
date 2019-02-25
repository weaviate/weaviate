package telemetry

import (
	"time"
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

// Reports function calls in the last <provided interval> seconds in CBOR format to the provided url.
// Contains a failsafe mechanism in the case the url is unreachable.
func (r *Reporter) Start() {
	if r.enabled {
		time.Sleep(time.Duration(r.interval) * time.Second)
		extractedLog := r.log.ExtractLoggedRequests()

		r.transformToOutputFormat(extractedLog)
	}
}

// Transform the logged function calls to a minimized output format to reduce network traffic
func (r *Reporter) transformToOutputFormat(logs *map[string]*RequestLog) {
	transformer := NewOutputTransformer()
	minimizedLogs := transformer.MinimizeFormat(logs)
	/*cborLogs := */ transformer.EncodeAsCBOR(minimizedLogs)
	// TODO:
	// make new outputTransformer
	// minimize logs
	// cborize logs
	// send + retry
	// failsafe
}

func NewOutputTransformer() *outputTransformer {
	return &outputTransformer{}
}

type outputTransformer struct {
}

func (o *outputTransformer) MinimizeFormat(logs *map[string]*RequestLog) *[]map[string]interface{} {
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

func (o *outputTransformer) EncodeAsCBOR(minimizedLogs *[]map[string]interface{}) {
	// cbor.encode(*minimizedLogs)
}
