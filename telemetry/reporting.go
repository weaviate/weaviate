package telemetry

import (
	"time"

	"github.com/ugorji/go/codec"
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
		time.Sleep(time.Duration(r.interval) * time.Second)
		extractedLog := r.log.ExtractLoggedRequests()
		r.AddTimeStamps(extractedLog)
		/*transformedLog, err := */ r.TransformToOutputFormat(extractedLog)

		//		if err != nil {
		//			// TODO: pending answer by Bob
		//		}
	}
}

func (r *Reporter) AddTimeStamps(extractedLog *map[string]*RequestLog) {
	timestamp := time.Now().Unix()

	for _, log := range *extractedLog {
		log.When = timestamp
	}
}

// TODO: cover with acceptance test
// Transform the logged function calls to a minimized output format to reduce network traffic.
func (r *Reporter) TransformToOutputFormat(logs *map[string]*RequestLog) ([]uint8, error) {
	transformer := NewOutputTransformer()

	minimizedLogs := transformer.MinimizeFormat(logs)

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

func NewOutputTransformer() *outputTransformer {
	return &outputTransformer{
		Canonical: false,
	}
}

type outputTransformer struct {
	Canonical bool
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

func (o *outputTransformer) EncodeAsCBOR(minimizedLogs *[]map[string]interface{}) ([]uint8, error) {
	encoded := make([]byte, 0, 64)

	cborHandle := new(codec.CborHandle)
	cborHandle.Canonical = o.Canonical

	encoder := codec.NewEncoderBytes(&encoded, cborHandle)

	err := encoder.Encode(*minimizedLogs)

	if err != nil {
		return nil, err
	}

	return encoded, nil
}
