package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
)

const (
	dateFormat = "2006-Jan-02"
	quotaExt   = ".quota"
)

// UserQuota represents the user quota
type UserQuota struct {
	Objects  map[string]int `json:"objects"`
	MaxLimit int            `json:"maxLimit,omitempty"`
}

// NewUserQuota returns a new user quota
func NewUserQuota() *UserQuota {
	return &UserQuota{
		Objects:  make(map[string]int),
		MaxLimit: maxLimit,
	}
}

// getCurrentDateInUTC fetches the current date in UTC format
func getCurrentDateInUTC() time.Time {
	currentTime := time.Now().UTC()
	return time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), 0, 0, 0, 0, currentTime.Location())
}

// Refresh parses the time in the path of the objects and filters them if they are stale
func (quota *UserQuota) Refresh() (updated bool) {
	objects := map[string]int{}
	for object, version := range quota.Objects {
		tokens := strings.Split(object, "/")
		if len(tokens) < 3 {
			updated = true
			continue
		}
		date := tokens[0]
		t, err := time.Parse(dateFormat, date)
		if err != nil {
			updated = true
			continue
		}
		if getCurrentDateInUTC().After(t.UTC()) {
			updated = true
			continue
		}
		objects[object] = version
	}
	quota.Objects = objects
	return
}

// Write encodes the quota to the provided writer
func (quota UserQuota) Write(w io.Writer) error {
	encoder := json.NewEncoder(w)
	return encoder.Encode(quota)
}

// parseUserQuota reads the user quota from the reader and parses it
func parseUserQuota(r io.Reader) (*UserQuota, error) {
	var quota UserQuota
	if err := json.NewDecoder(r).Decode(&quota); err != nil {
		return nil, err
	}
	return &quota, nil
}

// readUserQuota GETs the user quota, reads and parses it
func readUserQuota(ctx context.Context, user string) (*UserQuota, error) {
	reader, err := s3Client.GetObject(ctx, quotaBucket, user+quotaExt, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return parseUserQuota(reader)
}

// updateUserQuota PUTs the provided user quota to MinIO
func updateUserQuota(ctx context.Context, user string, userQuota *UserQuota) error {
	var buf bytes.Buffer
	if err := userQuota.Write(&buf); err != nil {
		return err
	}
	_, err := s3Client.PutObject(context.Background(), quotaBucket, user+quotaExt, bytes.NewReader(buf.Bytes()), int64(buf.Len()), minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}
