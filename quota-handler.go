package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7"
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
		log.Printf("[ERROR] unable to read the body; %v\n", err)
		http.Error(w, "error reading response body", http.StatusBadRequest)
		return
	}
	var jsonData map[string]interface{}
	if err = json.Unmarshal(body, &jsonData); err != nil {
		log.Printf("[ERROR] unable to unmarshal the body; %v\n", err)
		http.Error(w, "error marshalling response body", http.StatusBadRequest)
		return
	}
	records, ok := jsonData["Records"].([]interface{})
	if !ok || len(records) == 0 {
		log.Println("[ERROR] missing records in the request body")
		http.Error(w, "missing records in the request body", http.StatusBadRequest)
		return
	}
	record := records[0].(map[string]interface{})
	s3Data, ok := record["s3"].(map[string]interface{})
	if !ok {
		log.Println("[ERROR] missing records in the request body")
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
		fmt.Printf("[ERROR] unable to escape the path %v; %v\n", object, err)
		http.Error(w, "unable to escape the object path", http.StatusBadRequest)
		return
	}

	tokens := strings.Split(path, "/")
	if len(tokens) < 3 {
		fmt.Println("[ERROR] invalid path %v", path)
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	user := tokens[1]

	lock(user)
	defer unlock(user)

	var userQuota *UserQuota
	userQuota, err = readUserQuota(context.Background(), user)
	if err != nil {
		if minio.ToErrorResponse(err).Code != "NoSuchKey" {
			fmt.Printf("[ERROR] unable to GET the manifest for user %v; %v\n", user, err)
			http.Error(w, "manifest file cannot be read", http.StatusBadRequest)
			return
		}
		userQuota = NewUserQuota()
		userQuota.Objects[path] = 1
	} else {
		userQuota.Refresh()
		if _, ok := userQuota.Objects[path]; ok {
			userQuota.Objects[path]++
		} else {
			userQuota.Objects[path] = 1
		}
	}

	if len(userQuota.Objects) >= userQuota.MaxLimit {
		fmt.Printf("[WARNING] unable to update quota; max limit exceeded for user %v\n", user)
		http.Error(w, "max limit exceeded", http.StatusForbidden)
		return
	}

	if err := updateUserQuota(context.Background(), user, userQuota); err != nil {
		fmt.Printf("unable to update user quota for user %v; %v\n", user, err)
		http.Error(w, "unable to update user quota", http.StatusBadRequest)
	}

	fmt.Printf("[LOG] updated quota for %v\n", user)
}

// GET /quota/check/{user}
//
// - Reads the quota of the provided user
// - Refreshes the quota
// - Checks if it exceeds the max limit
func quotaCheckHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	user := vars["user"]

	lock(user)
	defer unlock(user)

	userQuota, err := readUserQuota(context.Background(), user)
	if err != nil {
		if minio.ToErrorResponse(err).Code != "NoSuchKey" {
			// new user
			return
		}
		http.Error(w, "unable to GET user quota", http.StatusInternalServerError)
		return
	}
	userQuota.Refresh()

	if len(userQuota.Objects) >= userQuota.MaxLimit {
		http.Error(w, "max limit exceeded", http.StatusForbidden)
	}
}

// GET /quota/refresh
//
// - Lists the user quotas from MinIO
// - Refreshes the user quota
// - PUTs the updated user quota back to MinIO
func quotaRefreshHandler(w http.ResponseWriter, r *http.Request) {
	refreshUserQuota := func(user string) error {
		lock(user)
		defer unlock(user)

		userQuota, err := readUserQuota(context.Background(), user)
		if err != nil {
			fmt.Printf("[ERROR] unable to read user quota for user %v; %v\n", user, err)
			return fmt.Errorf("unable to read user quota for user %v; %v\n", user, err)
		}
		if userQuota.Refresh() {
			if err := updateUserQuota(context.Background(), user, userQuota); err != nil {
				fmt.Printf("[ERROR] unable to update user quota for user %v; %v\n", user, err)
				return fmt.Errorf("unable to update user quota for user %v; %v\n", user, err)
			}
		}
		return nil
	}
	for object := range s3Client.ListObjects(context.Background(), quotaBucket, minio.ListObjectsOptions{}) {
		if object.Err != nil {
			fmt.Printf("[ERROR] unable to list objects from %v bucket; %v\n", quotaBucket, object.Err)
			http.Error(w, fmt.Sprintf("unable to list objects; %v", object.Err), http.StatusInternalServerError)
			return
		}
		user := strings.TrimSuffix(object.Key, quotaExt)
		if err := refreshUserQuota(user); err != nil {
			fmt.Println("[ERROR] " + err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Printf("[LOG] refreshed quota for %v\n", user)
	}
	return
}

// GET /purge
//
// - Lists all the voice mails
// - Checks if the objects fall behind the current time
// - If yes, force deletes them
// NOTE: Meant to be run in a CRON-JOB periodically every day
func purgeHandler(w http.ResponseWriter, r *http.Request) {
	for object := range s3Client.ListObjects(context.Background(), dataBucket, minio.ListObjectsOptions{}) {
		if object.Err != nil {
			fmt.Printf("[ERROR] unable to list objects from %v bucket; %v\n", dataBucket, object.Err)
			http.Error(w, fmt.Sprintf("unable to list objects; %v", object.Err), http.StatusInternalServerError)
			return
		}
		key := strings.TrimSuffix(object.Key, "/")
		t, err := time.Parse(dateFormat, key)
		if err != nil {
			fmt.Printf("[ERROR] unable to parse key %v; %v\n", key, err)
			continue
		}
		if getCurrentDateInUTC().After(t.UTC()) {
			if err := s3Client.RemoveObject(context.Background(), dataBucket, key, minio.RemoveObjectOptions{
				ForceDelete: true,
			}); err != nil {
				fmt.Printf("[ERROR] unable to delete the object from source: %v/%v; %v\n", dataBucket, key, err)
				continue
			}
			fmt.Printf("[LOG] purged %v/%v\n", dataBucket, key)
		}
	}
}
