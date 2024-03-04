package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/pkg/sync/errgroup"
)

const (
	dateFormat    = "2006-Jan-02"
	quotaExt      = ".quota"
	retryAttempts = 3
	retryTimeout  = 3 * time.Second
)

// UserQuota represents the user quota
type UserQuota struct {
	Objects  map[string]struct{} `json:"objects"`
	MaxLimit int                 `json:"maxLimit,omitempty"`
}

// NewUserQuota returns a new user quota
func NewUserQuota() *UserQuota {
	return &UserQuota{
		Objects:  make(map[string]struct{}),
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
	objects := map[string]struct{}{}
	for object, _ := range quota.Objects {
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
		objects[object] = struct{}{}
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
func readUserQuota(ctx context.Context, s3Client *minio.Client, user string) (*UserQuota, string, error) {
	reader, err := s3Client.GetObject(ctx, quotaBucket, user+quotaExt, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", err
	}
	defer reader.Close()

	stat, err := reader.Stat()
	if err != nil {
		return nil, "", err
	}
	etag := stat.ETag
	userQuota, err := parseUserQuota(reader)
	return userQuota, etag, err
}

// updateUserQuota PUTs the provided user quota to MinIO
func updateUserQuota(ctx context.Context, s3Client *minio.Client, user string, userQuota *UserQuota, etag string) error {
	var buf bytes.Buffer
	if err := userQuota.Write(&buf); err != nil {
		return err
	}
	opts := minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	}
	opts.SetMatchETag(etag)

	_, err := s3Client.PutObject(ctx,
		quotaBucket,
		user+quotaExt,
		bytes.NewReader(buf.Bytes()),
		int64(buf.Len()),
		opts)
	return err
}

// updateQuota updates the quota on all the s3clients configured
func updateQuota(ctx context.Context, user, path string) error {
	g := errgroup.WithNErrs(len(s3Clients))
	for index := range s3Clients {
		index := index
		g.Go(func() (err error) {
			if s3Clients[index] == nil {
				return errors.New("s3Client is nil")
			}
			for attempts := 1; attempts <= retryAttempts; attempts++ {
				err = updateLatestUserQuota(ctx, s3Clients[index], user, path)
				if err == nil {
					return
				}
				time.Sleep(retryTimeout)
			}
			return
		}, index)
	}
	return g.WaitErr()
}

func updateLatestUserQuota(ctx context.Context, s3Client *minio.Client, user, path string) error {
	userQuota, etag, err := readUserQuota(ctx, s3Client, user)
	if err != nil {
		if minio.ToErrorResponse(err).Code != "NoSuchKey" {
			fmt.Printf("[ERROR][%v] unable to GET the manifest for user '%v'; %v\n", s3Client.EndpointURL().Host, user, err)
			return fmt.Errorf("user quota cannot be read; %v", err)
		}
		userQuota = NewUserQuota()
		userQuota.Objects[path] = struct{}{}
	} else {
		if etag == "" {
			fmt.Printf("[ERROR][%v] ETag not returned for user quota; user: '%v';", s3Client.EndpointURL().Host, user)
			return fmt.Errorf("ETag not found in object; %v", err)
		}
		userQuota.Refresh()
		if _, ok := userQuota.Objects[path]; ok {
			// Already appended
			return nil
		} else {
			userQuota.Objects[path] = struct{}{}
		}
	}
	if len(userQuota.Objects) > userQuota.MaxLimit {
		fmt.Printf("[WARNING][%v] unable to update quota; max limit exceeded for user '%v'\n", s3Client.EndpointURL().Host, user)
		return errMaxLimitExceeded
	}
	if err := updateUserQuota(ctx, s3Client, user, userQuota, etag); err != nil {
		fmt.Printf("[ERROR][%v] unable to update user quota for user '%v'; %v\n", s3Client.EndpointURL().Host, user, err)
		return fmt.Errorf("unable to update user quota for user: %v; %v", user, err)
	}
	return nil
}

// checkQuota asks the s3clients to know if the userquota exceeded or not
func checkQuota(ctx context.Context, user string) error {
	g := errgroup.WithNErrs(len(s3Clients))
	for index := range s3Clients {
		index := index
		g.Go(func() (err error) {
			if s3Clients[index] == nil {
				return errors.New("s3Client is nil")
			}
			userQuota, _, err := readUserQuota(ctx, s3Clients[index], user)
			if err != nil {
				if minio.ToErrorResponse(err).Code != "NoSuchKey" {
					// new user
					return nil
				}
				return fmt.Errorf("unable to GET user quota; %v", err)
			}
			userQuota.Refresh()
			if len(userQuota.Objects) >= userQuota.MaxLimit {
				return errMaxLimitExceeded
			}
			return nil
		}, index)
	}
	var finalErr error
	for _, err := range g.Wait() {
		if err != nil {
			if errors.Is(err, errMaxLimitExceeded) {
				return err
			}
			finalErr = err
		}
	}
	return finalErr
}

// refreshQuota lists and refreshes the quota on all the s3clients configured
func refreshQuota(ctx context.Context) error {
	refreshUserQuota := func(s3Client *minio.Client, user string) error {
		userQuota, etag, err := readUserQuota(ctx, s3Client, user)
		if err != nil {
			fmt.Printf("[ERROR] unable to read user quota for user '%v'; %v\n", user, err)
			return fmt.Errorf("unable to read user quota for user '%v'; %v\n", user, err)
		}
		if etag == "" {
			fmt.Printf("[ERROR] ETag not returned for user quota; user: '%v';", user)
			return fmt.Errorf("ETag not found in object; %v", err)
		}
		if userQuota.Refresh() {
			if err := updateUserQuota(ctx, s3Client, user, userQuota, etag); err != nil {
				fmt.Printf("[ERROR] unable to update user quota for user '%v'; %v\n", user, err)
				return fmt.Errorf("unable to update user quota for user '%v'; %v\n", user, err)
			}
		}
		return nil
	}

	g := errgroup.WithNErrs(len(s3Clients))
	for index := range s3Clients {
		index := index
		g.Go(func() (err error) {
			if s3Clients[index] == nil {
				return errors.New("s3Client is nil")
			}
			for object := range s3Clients[index].ListObjects(ctx, quotaBucket, minio.ListObjectsOptions{}) {
				if object.Err != nil {
					fmt.Printf("[ERROR] unable to list objects from '%v' bucket; %v\n", quotaBucket, object.Err)
					return fmt.Errorf("unable to list objects; %v", object.Err)
				}
				user := strings.TrimSuffix(object.Key, quotaExt)
				var err error
				for attempts := 1; attempts <= retryAttempts; attempts++ {
					err = refreshUserQuota(s3Clients[index], user)
					if err == nil {
						fmt.Printf("[LOG] refreshed quota for user '%v'\n", user)
						break
					}
					fmt.Println("[ERROR] " + err.Error())
					time.Sleep(retryTimeout)
				}
			}
			return nil
		}, index)
	}

	return g.WaitErr()
}

// purge purges expired data objects on all the configured s3 clients
func purge(ctx context.Context) error {
	g := errgroup.WithNErrs(len(s3Clients))
	for index := range s3Clients {
		index := index
		g.Go(func() (err error) {
			if s3Clients[index] == nil {
				return errors.New("s3Client is nil")
			}
			for object := range s3Clients[index].ListObjects(context.Background(), dataBucket, minio.ListObjectsOptions{}) {
				if object.Err != nil {
					fmt.Printf("[ERROR] unable to list objects from '%v' bucket; %v\n", dataBucket, object.Err)
					return fmt.Errorf("unable to list objects; %v", object.Err)
				}
				key := strings.TrimSuffix(object.Key, "/")
				t, err := time.Parse(dateFormat, key)
				if err != nil {
					fmt.Printf("[ERROR] unable to parse key '%v'; %v\n", key, err)
					continue
				}
				if getCurrentDateInUTC().After(t.UTC()) {
					if err := s3Clients[index].RemoveObject(context.Background(), dataBucket, key, minio.RemoveObjectOptions{
						ForceDelete: true,
					}); err != nil {
						fmt.Printf("[ERROR] unable to delete the object from source: '%v/%v'; %v\n", dataBucket, key, err)
						continue
					}
					fmt.Printf("[LOG] purged '%v/%v'\n", dataBucket, key)
				}
			}
			return nil
		}, index)
	}
	return g.WaitErr()

}
