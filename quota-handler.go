package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

var (
	errMaxLimitExceeded = errors.New("max limit exceeded")
)

// POST /quota/update
//
// - Parse the incoming MinIO bucket notification PUT event of the file voicemails/DATE/USER/object
// - Reads the corresponding user quota of the user
// - If the quota is not present, will add a new quota file - `manifests/USER.quota` and adds the object path to the quota
// - If quota is present, will append the path to the quota objects list
func updateQuotaHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		fmt.Printf("[ERROR] unable to read the body; %v\n", err)
		http.Error(w, "error reading response body", http.StatusBadRequest)
		return
	}
	var jsonData map[string]interface{}
	if err = json.Unmarshal(body, &jsonData); err != nil {
		fmt.Printf("[ERROR] unable to unmarshal the body; %v\n", err)
		http.Error(w, "error marshalling response body", http.StatusBadRequest)
		return
	}
	records, ok := jsonData["Records"].([]interface{})
	if !ok || len(records) == 0 {
		fmt.Println("[ERROR] missing records in the request body")
		http.Error(w, "missing records in the request body", http.StatusBadRequest)
		return
	}
	record := records[0].(map[string]interface{})
	s3Data, ok := record["s3"].(map[string]interface{})
	if !ok {
		fmt.Println("[ERROR] missing records in the request body")
		http.Error(w, "missing s3 data in the request body", http.StatusBadRequest)
		return
	}
	var bucket string
	if bucketData, ok := s3Data["bucket"].(map[string]interface{}); ok {
		bucket, _ = bucketData["name"].(string)
	}
	var object string
	if objectData, ok := s3Data["object"].(map[string]interface{}); ok {
		object, _ = objectData["key"].(string)
	}
	if bucket == "" || object == "" {
		log.Println("[ERROR] bucket or object found to be empty")
		return
	}

	path, err := url.PathUnescape(object)
	if err != nil {
		fmt.Printf("[ERROR] unable to escape the path '%v'; %v\n", object, err)
		http.Error(w, "unable to escape the object path", http.StatusBadRequest)
		return
	}

	tokens := strings.Split(path, "/")
	if len(tokens) < 3 {
		fmt.Printf("[ERROR] invalid path '%v'\n", path)
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	date := tokens[0]
	user := tokens[1]

	t, err := time.Parse(dateFormat, date)
	if err != nil {
		fmt.Printf("[ERROR] unable to parse the date '%v' in the '%v'; %v\n", date, path, err)
		http.Error(w, "invalid path", http.StatusBadRequest)
	}
	if getCurrentDateInUTC().After(t.UTC()) {
		fmt.Printf("[ERROR] unable to update the quota; the date found in the path '%v' is older than the current date\n", path)
		// purposefully sending 200 OK because we don't want such events to be retried
		return
	}
	if err := updateQuota(context.Background(), user, path); err != nil {
		http.Error(w, fmt.Sprintf("unable to update quota; %v", err), http.StatusBadRequest)
		return
	}
	fmt.Printf("[LOG] updated quota for '%v'\n", user)
}

// GET /quota/check/{user}
//
// - Reads the quota of the provided user
// - Refreshes the quota
// - Checks if it exceeds the max limit
func quotaCheckHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	user := vars["user"]

	if err := checkQuota(context.Background(), user); err != nil {
		if errors.Is(err, errMaxLimitExceeded) {
			http.Error(w, err.Error(), http.StatusForbidden)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
}

// GET /quota/refresh
//
// - Lists the user quotas from MinIO
// - Refreshes the user quota
// - PUTs the updated user quota back to MinIO
func quotaRefreshHandler(w http.ResponseWriter, r *http.Request) {
	if err := refreshQuota(context.Background()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// DELETE /purge
//
// - Lists all the voice mails
// - Checks if the objects fall behind the current time
// - If yes, force deletes them
// NOTE: Meant to be run in a CRON-JOB periodically every day
func purgeHandler(w http.ResponseWriter, r *http.Request) {
	if err := purge(context.Background()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
