//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package gemini

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"

	mmapgo "github.com/edsrzf/mmap-go"
	"github.com/pkg/errors"
	"golang.org/x/exp/mmap"
)

const (
	DefaultSearchType       = "flat"
	DefaultTrainInd         = true
	DefaultGridTrain        = false
	DefaultNBits            = 768
	DefaultQBits            = 768
	DefaultTargetAccuracy   = 100
	DefaultMDUnique         = false
	DefaultConvertToDataset = false
)

const (
	DefaultTypicalNQueries   = 10
	DefaultMaxNQueries       = 3100
	DefaultNormalize         = false
	DefaultCentroidsHammingK = 5000
	DefaultCentroidsRerank   = 4000
	DefaultHammingK          = 3200
	DefaultTopK              = 1000
	DefaultBitmasksInd       = false
	DefaultAsyncLoad         = false
)

const (
	DefaultFVSPort = 7761
)

func Import_dataset(host string, port uint, allocation_token string, path string, bits uint, verbose bool) (string, error) {
	// form the rest url
	url := fmt.Sprintf("http://%s:%d/v1.0/dataset/import", host, port)
	if verbose {
		fmt.Println("Fvs: Import_dataset: url=", url)
	}

	// create the post json payload
	values := map[string]interface{}{
		"dsFilePath":       path,
		"searchType":       DefaultSearchType,
		"trainInd":         DefaultTrainInd,
		"gridTrain":        DefaultGridTrain,
		"nbits":            bits,
		"qbits":            DefaultQBits,
		"targetAccuracy":   DefaultTargetAccuracy,
		"mdUnique":         DefaultMDUnique,
		"convertToDataset": DefaultConvertToDataset,
	}
	jsonValue, jErr := json.Marshal(values)
	if jErr != nil {
		return "", jErr
	}
	if verbose {
		fmt.Println("Fvs: Import_dataset: body json=", jsonValue)
	}

	// form a request object
	request, rErr := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if rErr != nil {
		return "", errors.Wrap(rErr, "Fvs: Import_dataset could not create new http request.")
	}

	// add headers
	// {'allocationToken': '0b391a1a-b916-11ed-afcb-0242ac1c0002', 'Accept': 'application/json', 'Content-Type': 'application/json', 'User-Agent': 'Swagger-Codegen/1.0.0/python'}
	request.Header.Set("allocationToken", allocation_token)
	request.Header.Set("Accept", "application/json")       //; charset=UTF-8")
	request.Header.Set("Content-Type", "application/json") //; charset=UTF-8")
	request.Header.Set("User-Agent", "weaviate_gemini_plugin")

	// perform the request
	client := &http.Client{}
	response, dErr := client.Do(request)
	if dErr != nil {
		return "", errors.Wrap(dErr, "client.Do failed at Fvs Import_dataset.")
	}
	defer response.Body.Close()

	// retrieve response
	// respbody, _ := ioutil.ReadAll(response.Body)
	respbody, _ := io.ReadAll(response.Body)
	if verbose {
		fmt.Println("Fvs: Import_dataset: response status=", response.Status)
		fmt.Println("Fvs: Import_dataset: response headers=", response.Header)
		fmt.Println("Fvs: Import_dataset: response body=", string(respbody))
	}

	// parse the json response
	respData := map[string]interface{}{}
	juErr := json.Unmarshal(respbody, &respData)
	if juErr != nil {
		return "", errors.Wrap(juErr, "json.Unmarshal failed at Import_dataset.")
	}
	if verbose {
		fmt.Println("Fvs: Import_dataset: json resp=", respData, juErr)
	}

	// reconstruct the dataset id
	did, ok := respData["datasetId"].(string)
	if !ok {
		return "", fmt.Errorf("Response map does not have 'datasetId' key in Import_dataset.")
	}
	// TODO:  Check valid GUID format in 'did' string

	if verbose {
		fmt.Println("Fvs: Import_dataset : dataset id=", did)
	}

	return did, nil
}

func Train_status(host string, port uint, allocation_token string, dataset_id string, verbose bool) (string, error) {
	// form the rest url
	url := fmt.Sprintf("http://%s:%d/v1.0/dataset/train/status/%s", host, port, dataset_id)
	if verbose {
		fmt.Println("Fvs: Train_status: url=", url)
	}

	// form a request object
	request, rErr := http.NewRequest("GET", url, nil)
	if rErr != nil {
		return "error", errors.Wrap(rErr, "http.NewRequest failed in Fvs Train_status.")
	}

	// add headers
	request.Header.Set("allocationToken", allocation_token)
	request.Header.Set("Accept", "application/json")       //; charset=UTF-8")
	request.Header.Set("Content-Type", "application/json") //; charset=UTF-8")
	request.Header.Set("User-Agent", "weaviate_gemini_plugin")

	// perform the request
	client := &http.Client{}
	response, dErr := client.Do(request)
	if dErr != nil {
		return "", errors.Wrap(dErr, "client.Do failed in Fvs Train_status")
	}
	defer response.Body.Close()

	// retrieve response
	// respbody, _ := ioutil.ReadAll(response.Body)
	respbody, _ := io.ReadAll(response.Body)
	if verbose {
		fmt.Println("Fvs: Train_status: response status=", response.Status)
		fmt.Println("Fvs: Train_status: response headers=", response.Header)
		fmt.Println("Fvs: Train_status: response body=", string(respbody))
	}

	// parse the json response
	respData := map[string]interface{}{}
	juErr := json.Unmarshal(respbody, &respData)
	if juErr != nil {
		return "", errors.Wrap(juErr, "json.Unmarshal failed in Fvs Train_status.")
	}
	if verbose {
		fmt.Println("Fvs: Train_status: json resp=", respData, juErr)
	}

	// check http status
	if response.Status != "200 OK" {
		return "", fmt.Errorf("Invalid response from FVS server - %v", response.Status)
	}

	// reconstruct the queries id returned
	status, ok := respData["datasetStatus"].(string)
	if !ok {
		return "", fmt.Errorf("Response map does not have 'datasetStatus' key in Fvs Train_status.")
	}
	if verbose {
		fmt.Println("Fvs: Train_status: status=", status)
	}
	// TODO: Should we convert status to an error if it represents an error?

	return status, nil
}

func Load_dataset(host string, port uint, allocation_token string, dataset_id string, verbose bool) (string, error) {
	// form the rest url
	url := fmt.Sprintf("http://%s:%d/v1.0/dataset/load", host, port)
	if verbose {
		fmt.Println("Fvs: Load_dataset: url=", url)
	}

	// create the post json payload
	values := map[string]interface{}{
		"allocationId":      allocation_token,
		"datasetId":         dataset_id,
		"typicalNQueries":   DefaultTypicalNQueries,
		"maxNQueries":       DefaultMaxNQueries,
		"normalize":         DefaultNormalize,
		"centroidsHammingK": DefaultCentroidsHammingK,
		"centroidsRerank":   DefaultCentroidsRerank,
		"hammingK":          DefaultHammingK,
		"topk":              DefaultTopK,
		"bitmasksInd":       DefaultBitmasksInd,
		"asyncLoad":         DefaultAsyncLoad,
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return "", errors.Wrap(err, "json.Marshal failed in Fvs Load_dataset")
	}
	if verbose {
		fmt.Println("Fvs: Load_dataset: body json=", jsonValue)
	}

	// form a request object
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if err != nil {
		return "", errors.Wrap(err, "http.NewRequest failed in Fvs Load_dataset")
	}

	// add headers
	request.Header.Set("allocationToken", allocation_token)
	request.Header.Set("Accept", "application/json")       //; charset=UTF-8")
	request.Header.Set("Content-Type", "application/json") //; charset=UTF-8")
	request.Header.Set("User-Agent", "weaviate_gemini_plugin")

	// perform the request
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return "", errors.Wrap(err, "client.Do failed in Fvs Load_dataset.")
	}
	defer response.Body.Close()

	// retrieve response
	// respbody, _ := ioutil.ReadAll(response.Body)
	respbody, _ := io.ReadAll(response.Body)
	if verbose {
		fmt.Println("Fvs: Load_dataset: response status=", response.Status)
		fmt.Println("Fvs: Load_dataset: response headers=", response.Header)
		fmt.Println("Fvs: Load_dataset: response body=", string(respbody))
	}

	// parse the json response
	respData := map[string]interface{}{}
	rErr := json.Unmarshal(respbody, &respData)
	if rErr != nil {
		return "", errors.Wrap(rErr, "json.Unmarshal failed at Fvs Load_dataset.")
	}
	if verbose {
		fmt.Println("Fvs: Load_dataset: json resp=", respData, rErr)
	}

	status := respData["status"].(string)
	if verbose {
		fmt.Println("Fvs: Load_dataset: status=", status)
	}
	// TODO: Should a status != "OK" be translated to an error?

	return status, nil
}

func Import_queries(host string, port uint, allocation_token string, path string, verbose bool) (string, error) {
	// form the rest url
	url := fmt.Sprintf("http://%s:%d/v1.0/demo/query/import", host, port)
	if verbose {
		fmt.Println("Fvs: Import_queries: url=", url)
	}

	// create the post json payload
	values := map[string]interface{}{
		"queriesFilePath": path,
	}
	jsonValue, jErr := json.Marshal(values)
	if jErr != nil {
		return "", errors.Wrap(jErr, "json.Marshal failed at Fvs Import_queries.")
	}
	if verbose {
		fmt.Println("Fvs: Import_queries: body json=", jsonValue)
	}

	// form a request object
	request, rErr := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if rErr != nil {
		return "", errors.Wrap(rErr, "http.NewRequest failed at Fvs Import_queries.")
	}

	// add headers
	request.Header.Set("allocationToken", allocation_token)
	request.Header.Set("Accept", "application/json")       //; charset=UTF-8")
	request.Header.Set("Content-Type", "application/json") //; charset=UTF-8")
	request.Header.Set("User-Agent", "weaviate_gemini_plugin")

	// perform the request
	client := &http.Client{}
	response, dErr := client.Do(request)
	if dErr != nil {
		return "", dErr
	}
	defer response.Body.Close()

	// retrieve response
	// respbody, _ := ioutil.ReadAll(response.Body)
	respbody, _ := io.ReadAll(response.Body)
	if verbose {
		fmt.Println("Fvs: Import_queries: response status=", response.Status)
		fmt.Println("Fvs: Import_queries: response headers=", response.Header)
		fmt.Println("Fvs: Import_queries: response body=", string(respbody))
	}

	// parse the json response
	respData := map[string]interface{}{}
	juErr := json.Unmarshal(respbody, &respData)
	if juErr != nil {
		return "", juErr
	}
	if verbose {
		fmt.Println("Fvs: Import_queries: json resp=", respData, rErr)
	}

	// reconstruct the queries id returned
	aq := respData["addedQuery"].(map[string]interface{})
	qid := aq["id"].(string)
	if verbose {
		fmt.Println("Fvs: Import_queries : query id=", qid)
	}

	return qid, nil
}

func Set_focus(host string, port uint, allocation_token string, dataset_id string, verbose bool) error {
	// form the rest url
	url := fmt.Sprintf("http://%s:%d/v1.0/dataset/focus", host, port)
	if verbose {
		fmt.Println("Fvs: Set_focust: url=", url)
	}

	// create the post json payload
	//{'allocationId': '0b391a1a-b916-11ed-afcb-0242ac1c0002', 'datasetId': 'cbd7c113-b700-4b32-9bf6-0b9205b7525e'}
	values := map[string]interface{}{
		"allocationId": allocation_token,
		"datasetId":    dataset_id,
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return errors.Wrap(err, "json.Marshal failed in Fvs Set_focus.")
	}
	if verbose {
		fmt.Println("Fvs: Set_focus: body json=", jsonValue)
	}

	// form a request object
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if err != nil {
		return errors.Wrap(err, "http.NewRequest failed in Fvs Set_focus.")
	}

	// add headers
	request.Header.Set("allocationToken", allocation_token)
	request.Header.Set("Accept", "application/json")       //; charset=UTF-8")
	request.Header.Set("Content-Type", "application/json") //; charset=UTF-8")
	request.Header.Set("User-Agent", "weaviate_gemini_plugin")

	// perform the request
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return errors.Wrap(err, "client.Do failed in Fvs Set_focus.")
	}
	defer response.Body.Close()

	// retrieve response
	// respbody, _ := ioutil.ReadAll(response.Body)
	respbody, _ := io.ReadAll(response.Body)
	if verbose {
		fmt.Println("Fvs: Set_focus: response status=", response.Status)
		fmt.Println("Fvs: Set_focus: response headers=", response.Header)
		fmt.Println("Fvs: Set_focus: response body=", string(respbody))
	}

	// parse the json response
	respData := map[string]interface{}{}
	rErr := json.Unmarshal(respbody, &respData)
	if rErr != nil {
		return errors.Wrap(rErr, "json.Unmarshal failed at Fvs Set_focus.")
	}
	if verbose {
		fmt.Println("Fvs: Set_focus: json resp=", respData, rErr)
	}

	return nil
}

func Search(host string, port uint, allocation_token string, dataset_id string, path string, topk uint, verbose bool) ([][]float32, [][]uint64, float32, error) {
	// form the rest url
	url := fmt.Sprintf("http://%s:%d/v1.0/dataset/search", host, port)
	if verbose {
		fmt.Println("Fvs: Search: url=", url)
	}

	// compose the post body
	values := map[string]interface{}{
		"allocationId":    allocation_token,
		"datasetId":       dataset_id,
		"queriesFilePath": path,
		"topk":            topk,
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "json.Marhal failed at Fvs Search.")
	}
	if verbose {
		fmt.Println("Fvs: Search: body json=", jsonValue)
	}

	// form a request object
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "http.NewRequest failed at Fvs Search.")
	}

	// add headers
	request.Header.Set("allocationToken", allocation_token)
	request.Header.Set("Accept", "application/json")       //; charset=UTF-8")
	request.Header.Set("Content-Type", "application/json") //; charset=UTF-8")
	request.Header.Set("User-Agent", "weaviate_gemini_plugin")

	// perform the request
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return nil, nil, 0, errors.Wrap(err, "client.Do failed in Fvs Search.")
	}
	defer response.Body.Close()

	// retrieve response
	// respbody, _ := ioutil.ReadAll(response.Body)
	respbody, _ := io.ReadAll(response.Body)
	if verbose {
		fmt.Println("Fvs: Search: response status=", response.Status)
		fmt.Println("Fvs: Search: response headers=", response.Header)
		fmt.Println("Fvs: Search: response body=", string(respbody))
	}

	// check http status
	if response.Status != "200 OK" {
		return nil, nil, 0, fmt.Errorf("Invalid response from FVS server - %v", response.Status)
	}

	// parse the json response
	respData := map[string]interface{}{}
	rErr := json.Unmarshal(respbody, &respData)
	if rErr != nil {
		return nil, nil, 0, errors.Wrap(rErr, "json.Unmarshal failed at Fvs Search.")
	}
	if verbose {
		fmt.Println("Fvs: Search: json resp=", respData, rErr)
	}

	// reconstruct the distances returned
	dist, ok := respData["distance"].([]interface{})
	if !ok {
		return nil, nil, 0, fmt.Errorf("Response map does not have 'distance' key in Fvs Search.")
	}
	farr := make([][]float32, len(dist))
	for i := 0; i < len(dist); i++ {
		inner := dist[i].([]interface{})
		farr[i] = make([]float32, len(inner))
		for j := 0; j < len(inner); j++ {
			switch inner[j].(type) {
			case string: // THIS WORKS WITH THE REAL FVS SERVER
				ff, fErr := strconv.ParseFloat(inner[j].(string), 32)
				if fErr != nil {
					return nil, nil, 0, errors.Wrap(fErr, "float32 extraction failed")
				}
				farr[i][j] = float32(ff)
			case float64: // THIS WORKS WITH FAKE_FVS
				farr[i][j] = float32(inner[j].(float64))
			default:
				return nil, nil, 0, fmt.Errorf("unsupported data type")
			}
		}
	}
	if verbose {
		fmt.Println("Fvs: Search: reconstructed dists=", farr)
	}

	// reconstruct the indices returned
	inds, ok := respData["indices"].([]interface{})
	if !ok {
		return nil, nil, 0, fmt.Errorf("Response map does not have 'indices' key in Fvs Search.")
	}
	iarr := make([][]uint64, len(inds))
	for i := 0; i < len(inds); i++ {
		inner := inds[i].([]interface{})
		iarr[i] = make([]uint64, len(inner))
		for j := 0; j < len(inner); j++ {
			iarr[i][j] = uint64(inner[j].(float64))
		}
	}
	if verbose {
		fmt.Println("Fvs: Search: reconstructed inds=", iarr)
	}

	// reconstruct the timing
	timing64, ok := respData["search"].(float64)
	timing := float32(timing64)
	if !ok {
		return nil, nil, 0, fmt.Errorf("Response map does not have 'timing' key in Fvs Search.")
	}
	if verbose {
		fmt.Println("Fvs: Search: reconstructed timing=", timing)
	}

	return farr, iarr, timing, nil
}

func Delete_queries(host string, port uint, allocation_token string, qid string, verbose bool) (string, error) {
	// form the rest url
	url := fmt.Sprintf("http://%s:%d/v1.0/demo/query/remove/%s", host, port, qid)

	// form a request object
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return "", errors.Wrap(err, "http.NewRequest failed at Fvs Delete_queries.")
	}

	// add headers
	request.Header.Set("allocationToken", allocation_token)
	request.Header.Set("Content-Type", "application/json") //; charset=UTF-8")

	// perform the request
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return "", errors.Wrap(err, "client.Do failed in Fvs_delete_queries.")
	}
	defer response.Body.Close()

	// retrieve response
	// respbody, _ := ioutil.ReadAll(response.Body)
	respbody, _ := io.ReadAll(response.Body)
	if verbose {
		fmt.Println("Fvs: Delete_queries: response status=", response.Status)
		fmt.Println("Fvs: Delete_queries: response headers=", response.Header)
		fmt.Println("Fvs: Delete_queries: response body=", string(respbody))
	}

	// parse the json response
	respData := map[string]interface{}{}
	rErr := json.Unmarshal(respbody, &respData)
	if rErr != nil {
		return "", rErr
	}
	if verbose {
		fmt.Println("Fvs: Delete_queries: json resp=", respData, rErr)
	}

	// TODO: Do we need to return a status string in addition to error?
	return "ok", nil
}

func Unload_dataset(host string, port uint, allocation_token string, dataset_id string, verbose bool) (string, error) {
	// form the rest url
	url := fmt.Sprintf("http://%s:%d/v1.0/dataset/unload", host, port)
	if verbose {
		fmt.Println("Fvs: Unload_dataset: url=", url)
	}

	// create the post json payload
	values := map[string]interface{}{
		"allocationId": allocation_token,
		"datasetId":    dataset_id,
		"asyncUnload":  false,
	}
	jsonValue, err := json.Marshal(values)
	if err != nil {
		return "", errors.Wrap(err, "json.Marshal failed at Fvs Unload_dataset.")
	}
	if verbose {
		fmt.Println("Fvs: Unload_dataset: body json=", jsonValue)
	}

	// form a request object
	request, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonValue))
	if err != nil {
		return "", errors.Wrap(err, "http.NewRequest failed at Fvs Unload_dataset.")
	}

	// add headers
	request.Header.Set("allocationToken", allocation_token)
	request.Header.Set("Accept", "application/json")       //; charset=UTF-8")
	request.Header.Set("Content-Type", "application/json") //; charset=UTF-8")
	request.Header.Set("User-Agent", "weaviate_gemini_plugin")

	// perform the request
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return "", errors.Wrap(err, "client.Do failed in Fvs Unload_dataset.")
	}
	defer response.Body.Close()

	// retrieve response
	// respbody, _ := ioutil.ReadAll(response.Body)
	respbody, _ := io.ReadAll(response.Body)
	if verbose {
		fmt.Println("Fvs: Unload_dataset: response status=", response.Status)
		fmt.Println("Fvs: Unload_dataset: response headers=", response.Header)
		fmt.Println("Fvs: Unload_dataset: response body=", string(respbody))
	}

	// parse the json response
	respData := map[string]interface{}{}
	rErr := json.Unmarshal(respbody, &respData)
	if rErr != nil {
		return "", rErr
	}
	if verbose {
		fmt.Println("Fvs: Unload_dataset: json resp=", respData, rErr)
	}

	status := respData["status"].(string)
	if verbose {
		fmt.Println("Fvs: Unload_dataset: status=", status)
	}

	return status, nil
}

func Delete_dataset(host string, port uint, allocation_token string, dataset_id string, verbose bool) (string, error) {
	// form the rest url
	url := fmt.Sprintf("http://%s:%d/v1.0/dataset/remove/%s", host, port, dataset_id)

	// form a request object
	request, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return "", err
	}

	// add headers
	request.Header.Set("allocationToken", allocation_token)
	request.Header.Set("Content-Type", "application/json") //; charset=UTF-8")

	// perform the request
	client := &http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return "", errors.Wrap(err, "client.Do failed in Fvs Delete_dataset.")
	}
	defer response.Body.Close()

	// retrieve response
	// respbody, _ := ioutil.ReadAll(response.Body)
	respbody, _ := io.ReadAll(response.Body)
	if verbose {
		fmt.Println("Fvs: Delete_dataset: response status=", response.Status)
		fmt.Println("Fvs: Delete_dataset: response headers=", response.Header)
		fmt.Println("Fvs: Delete_dataset: response body=", string(respbody))
	}

	// parse the json response
	respData := map[string]interface{}{}
	rErr := json.Unmarshal(respbody, &respData)
	if rErr != nil {
		return "", rErr
	}
	if verbose {
		fmt.Println("Fvs: Delete_dataset: json resp=", respData, rErr)
	}

	status := respData["status"].(string)
	if verbose {
		fmt.Println("Fvs: Delete_dataset: status=", status)
	}

	return status, nil
}

// Read a uint32 array from data stored in numpy format
func Numpy_read_uint32_array(f *mmap.ReaderAt, arr [][]uint32, dim int64, index int64, count int64, offset int64) (int64, error) {
	// Iterate rows
	for j := 0; j < int(count); j++ {
		// Read consecutive 4 byte array into uint32 array, up to dims
		for i := 0; i < len(arr[j]); i++ {

			// Declare 4-byte array
			bt := []byte{0, 0, 0, 0}

			// Compute offset for next uint32
			r_offset := offset + (int64(j)+index)*dim*4 + int64(i)*4

			_, err := f.ReadAt(bt, r_offset)
			if err != nil {
				return 0, errors.Wrapf(err, "error reading file at offset: %d, %v", r_offset, err)
			}

			arr[j][i] = binary.LittleEndian.Uint32(bt)
		}
	}

	return dim, nil
}

// Read a float32 array from data stored in numpy format
func Numpy_read_float32_array(f *mmap.ReaderAt, arr [][]float32, dim int64, index int64, count int64, offset int64) (int64, error) {
	// Iterate rows
	for j := 0; j < int(count); j++ {
		// Read consecutive 4 byte array into uint32 array, up to dims
		for i := 0; i < len(arr[j]); i++ {

			// Declare 4-byte array
			bt := []byte{0, 0, 0, 0}

			// Compute offset for next uint32
			r_offset := offset + (int64(j)+index)*dim*4 + int64(i)*4

			_, err := f.ReadAt(bt, r_offset)
			if err != nil {
				return 0, fmt.Errorf("error reading file at offset:%v, %v", r_offset, err)
			}

			bits := binary.LittleEndian.Uint32(bt)
			arr[j][i] = math.Float32frombits(bits)
		}
	}

	return dim, nil
}

// Write a uint32 array to a file in numpy format
func Numpy_append_uint32_array(fname string, arr [][]uint32, dim int64, count int64) error {
	preheader := []byte{0x93, 0x4e, 0x55, 0x4d, 0x50, 0x59, 0x01, 0x00, 0x76, 0x00}
	fmt_header := "{'descr': '<i4', 'fortran_order': False, 'shape': (%d, %d), }"
	empty := []byte{0x20}
	fin := []byte{0x0a}

	// Check if file exists
	fexists := true
	_, err := os.Stat(fname)
	if os.IsNotExist(err) {
		fexists = false
	}

	// Get file descriptor
	var f *os.File = nil
	if fexists {
		// Open file
		f, err = os.OpenFile(fname, os.O_RDWR, 0o755)
		if err != nil {
			return fmt.Errorf("error openingfile: %v", err)
		}
	} else {
		// Create file
		f, err = os.Create(fname)
		if err != nil {
			return errors.Wrap(err, "error creating file in Numpy_append_uint32_array")
		}
		// Create header area
		err = f.Truncate(int64(128))
		if err != nil {
			return errors.Wrap(err, "error resizing file for header in Numpy_append_uint32_array")
		}
	}
	defer f.Close()

	// Get file size
	fi, err := f.Stat()
	if err != nil {
		return errors.Wrap(err, "error get file stats in Numpy_append_uint32_array.")
	}
	file_size := int64(fi.Size())

	// Get row count
	data_size := file_size - 128
	row_count := data_size / (dim * 4)
	new_row_count := row_count + count

	// Resize file
	new_size := file_size + dim*4*count
	err = f.Truncate(int64(new_size))
	if err != nil {
		return fmt.Errorf("error resizing file in Numpy_append_uint32_array: %v", err)
	}

	// Memory map the new file
	mem, err := mmapgo.Map(f, mmapgo.RDWR, 0)
	if err != nil {
		return errors.Wrap(err, "error mmapgo.Map in Numpy_append_uint32_array")
	}
	defer mem.Unmap()

	// Create the new header
	header := fmt.Sprintf(fmt_header, new_row_count, dim)

	// Write the numpy header info
	idx := 0
	for i := 0; i < len(preheader); i++ {
		mem[idx] = preheader[i]
		idx += 1
	}
	for i := 0; i < len(header); i++ {
		mem[idx] = header[i]
		idx += 1
	}
	for i := idx; i < 128; i++ {
		mem[idx] = empty[0]
		idx += 1
	}
	mem[127] = fin[0]

	// append the arrays
	idx = int(128 + data_size)
	for j := 0; j < int(count); j++ {
		for i := 0; i < len(arr[j]); i++ {
			bt := []byte{0, 0, 0, 0}
			binary.LittleEndian.PutUint32(bt, arr[j][i])
			mem[idx] = bt[0]
			mem[idx+1] = bt[1]
			mem[idx+2] = bt[2]
			mem[idx+3] = bt[3]
			idx += 4
		}
	}

	mem.Flush()

	return nil
}

// Write a float32 array to a file in numpy format
func Numpy_append_float32_array(fname string, arr [][]float32, dim int64, count int64) (int, int, error) {
	preheader := []byte{0x93, 0x4e, 0x55, 0x4d, 0x50, 0x59, 0x01, 0x00, 0x76, 0x00}
	fmt_header := "{'descr': '<f4', 'fortran_order': False, 'shape': (%d, %d), }"
	empty := []byte{0x20}
	fin := []byte{0x0a}

	// Check if file exists
	fexists := true
	_, err := os.Stat(fname)
	if os.IsNotExist(err) {
		fexists = false
	}

	// Get file descriptor
	var f *os.File = nil
	if fexists {
		// Open file
		f, err = os.OpenFile(fname, os.O_RDWR, 0o755)
		if err != nil {
			return 0, 0, errors.Wrap(err, "error openingfile in Numpy_append_float32_array.")
		}
	} else {
		// Create file
		f, err = os.Create(fname)
		if err != nil {
			return 0, 0, errors.Wrap(err, "error creating file in Numpy_append_float32_array.")
		}
		// Create header area
		err = f.Truncate(int64(128))
		if err != nil {
			return 0, 0, errors.Wrap(err, "error resizing file for header in Numpy_append_float32_array.")
		}
	}
	defer f.Close()

	// Get file size
	fi, err := f.Stat()
	if err != nil {
		log.Fatalf("error get file stats: %v", err)
	}
	prev_file_size := int64(fi.Size())

	// Get row count
	data_size := prev_file_size - 128
	row_count := data_size / (dim * 4)
	new_row_count := row_count + count

	// Resize file
	new_size := prev_file_size + dim*4*count
	err = f.Truncate(int64(new_size))
	if err != nil {
		log.Fatalf("error resizing file: %v", err)
	}

	// Memory map the new file
	mem, err := mmapgo.Map(f, mmapgo.RDWR, 0)
	if err != nil {
		return 0, 0, errors.Wrap(err, "error with mmapgo.Map in Numpy_append_float32_array")
	}
	defer mem.Unmap()
	// Create the new header
	header := fmt.Sprintf(fmt_header, new_row_count, dim)

	// Write the numpy header info
	idx := 0
	for i := 0; i < len(preheader); i++ {
		mem[idx] = preheader[i]
		idx += 1
	}
	for i := 0; i < len(header); i++ {
		mem[idx] = header[i]
		idx += 1
	}
	for i := idx; i < 128; i++ {
		mem[idx] = empty[0]
		idx += 1
	}
	mem[127] = fin[0]

	// fast-forward memmap index to the insert point
	idx = int(prev_file_size)

	// append the arrays
	for j := 0; j < int(count); j++ {
		for i := 0; i < len(arr[j]); i++ {
			bt := []byte{0, 0, 0, 0}
			//if j==0 {
			//    fmt.Printf("%d,%d:%f ", j,i, arr[j][i])
			//}
			tmp_uint32 := math.Float32bits(arr[j][i])
			binary.LittleEndian.PutUint32(bt, tmp_uint32)
			mem[idx] = bt[0]
			mem[idx+1] = bt[1]
			mem[idx+2] = bt[2]
			mem[idx+3] = bt[3]
			idx += 4
		}
	}

	mem.Flush()

	return int(new_row_count), int(dim), nil
}
