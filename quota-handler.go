package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/minio/minio-go/v7"
)

const (
	retentionDays = 10
	maxLimit      = 10
)

var (
	internalError = errors.New("internal error")
	requestError  = errors.New("invalid request")
)

type UserQuota struct {
	Objects  map[string]int `json:"objects"`
	MaxLimit int            `json:"maxLimit,omitempty"`
}

func NewUserQuota() *UserQuota {
	return &UserQuota{
		Objects:  make(map[string]int),
		MaxLimit: maxLimit,
	}
}

func parseUserQuota(r io.Reader) (*UserQuota, error) {
	var quota UserQuota
	if err := json.NewDecoder(r).Decode(&quota); err != nil {
		return nil, err
	}
	return &quota, nil
}

func (quota UserQuota) Write(w io.Writer) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(quota)
}

func readUserQuota(ctx context.Context, user string) (*UserQuota, error) {
	reader, err := s3Client.GetObject(ctx, "manifests", user+".quota", minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return parseUserQuota(reader)
}

// const shortForm = "2006-Jan-02"
// 	t, _ := time.Parse(shortForm, "2024-Feb-21")

func updateQuotaHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.Printf("unable to read the body; %v\n", err)
		http.Error(w, "error reading response body", http.StatusBadRequest)
		return
	}
	var jsonData map[string]interface{}
	if err = json.Unmarshal(body, &jsonData); err != nil {
		log.Printf("unable to unmarshal the body; %v\n", err)
		http.Error(w, "error marshalling response body", http.StatusBadRequest)
		return
	}
	records, ok := jsonData["Records"].([]interface{})
	if !ok || len(records) == 0 {
		log.Println("missing records in the request body")
		http.Error(w, "missing records in the request body", http.StatusBadRequest)
		return
	}
	record := records[0].(map[string]interface{})
	s3Data, ok := record["s3"].(map[string]interface{})
	if !ok {
		log.Println("missing records in the request body")
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
		return
	}
	fmt.Println(bucket)
	path, err := url.PathUnescape(object)
	if err != nil {
		fmt.Println("\n[ERROR] unable to escape the path %v; %v", object, err)
		http.Error(w, "unable to escape the object path", http.StatusBadRequest)
		return
	}
	fmt.Println(path)
	// 2021-Feb-20/usera/voice1.vm

	tokens := strings.Split(path, "/")
	if len(tokens) < 3 {
		http.Error(w, "invalid path", http.StatusBadRequest)
		return
	}
	user := tokens[1]
	var userQuota *UserQuota

	userQuota, err = readUserQuota(context.Background(), user)
	if err != nil {
		if minio.ToErrorResponse(err).Code != "NoSuchKey" {
			fmt.Printf("unable to GET the manifest; %v\n", err)
			http.Error(w, "manifest file cannot be read", http.StatusBadRequest)
			return
		}
		fmt.Println("creating new")
		userQuota = NewUserQuota()
	} else {
		if _, ok := userQuota.Objects[path]; ok {
			return
		}
	}
	userQuota.Objects[path] = retentionDays

	var buf bytes.Buffer
	if err := userQuota.Write(&buf); err != nil {
		http.Error(w, "unable to write user quota", http.StatusInternalServerError)
		return
	}
	info, err := s3Client.PutObject(context.Background(), "manifests", user+".quota", bytes.NewReader(buf.Bytes()), int64(buf.Len()), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	if err != nil {
		fmt.Printf("unable to PUT userquota for user %v; %v", user, err)
		http.Error(w, "unable to PUT user quota", http.StatusInternalServerError)
		return
	}
	fmt.Println(info)
}
