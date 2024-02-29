package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/pkg/env"
)

var (
	address     string
	authToken   = env.Get("WEBHOOK_AUTH_TOKEN", "")
	insecure    = env.Get("MINIO_INSECURE", "false") == "true"
	dataBucket  = env.Get("DATA_BUCKET", "")
	quotaBucket = env.Get("QUOTA_BUCKET", "")
	s3Clients   []*minio.Client
	dryRun      bool
	maxLimit    int
)

func main() {
	flag.StringVar(&address, "address", ":8080", "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname")
	flag.BoolVar(&dryRun, "dry-run", false, "Enable dry run mode")
	flag.Parse()

	var err error
	maxLimit, err = env.GetInt("MAX_OBJECT_LIMIT_PER_USER", 0)
	if err != nil {
		log.Fatalf("unable to read MAX_OBJECT_LIMIT_PER_USER env; %v", err)
	}
	if dataBucket == "" {
		log.Fatal("DATA_BUCKET env is not set")
	}
	if quotaBucket == "" {
		log.Fatal("QUOTA_BUCKET env is not set")
	}
	if maxLimit <= 0 {
		log.Fatalf("MAX_OBJECT_LIMIT_PER_USER env is not set")
	}

	envs := env.List("MINIO_ENDPOINT_")
	for _, k := range envs {
		targetName := strings.TrimPrefix(k, "MINIO_ENDPOINT_")
		endpoint := env.Get("MINIO_ENDPOINT_"+targetName, "")
		accessKey := env.Get("MINIO_ACCESS_"+targetName, "")
		secretKey := env.Get("MINIO_SECRET_"+targetName, "")
		if endpoint == "" {
			log.Fatalf("MINIO_ENDPOINT_%v is empty", targetName)
		}
		if accessKey == "" {
			log.Fatalf("MINIO_ACCESS_%v is not set", targetName)
		}
		if secretKey == "" {
			log.Fatalf("MINIO_SECRET_%v is not set", targetName)
		}
		insecure := env.Get("MINIO_INSECURE_"+targetName, strconv.FormatBool(insecure)) == "true"
		s3Client, err := getS3Client(endpoint, accessKey, secretKey, insecure)
		if err != nil {
			log.Fatalf("unable to create s3 client for site %v; %v", targetName, err)
		}
		found, err := s3Client.BucketExists(context.Background(), dataBucket)
		if err != nil {
			log.Fatalf("unable to stat the bucket %v in %v; %v", dataBucket, s3Client.EndpointURL().Host, err)
		}
		if !found {
			log.Fatalf("DATA_BUCKET %v does not exist in %v", dataBucket, s3Client.EndpointURL().Host)
		}
		found, err = s3Client.BucketExists(context.Background(), quotaBucket)
		if err != nil {
			log.Fatalf("unable to stat the bucket %v in %v; %v", quotaBucket, s3Client.EndpointURL().Host, err)
		}
		if !found {
			log.Fatalf("QUOTA_BUCKET %v does not exist in %v", quotaBucket, s3Client.EndpointURL().Host)
		}
		s3Clients = append(s3Clients, s3Client)
	}
	if len(s3Clients) == 0 {
		log.Fatal("no MinIO sites provided")
	}

	router := mux.NewRouter()

	router.Handle("/quota/update", auth(http.HandlerFunc(updateQuotaHandler))).Methods("POST")
	router.Handle("/quota/check/{user}", auth(http.HandlerFunc(quotaCheckHandler))).Methods("GET")
	router.Handle("/quota/refresh", auth(http.HandlerFunc(quotaRefreshHandler)))
	router.Handle("/purge", auth(http.HandlerFunc(purgeHandler))).Methods("DELETE")

	for _, s3Client := range s3Clients {
		fmt.Printf("Configured MinIO Site: %v\n", s3Client.EndpointURL().Host)
	}
	fmt.Printf("Configured data bucket: %v\n", dataBucket)
	fmt.Printf("Configured quota bucket: %v\n", quotaBucket)
	fmt.Printf("Configured max limit per user: %v\n", maxLimit)
	fmt.Println()
	fmt.Printf("Listening on %v ...\n", address)
	fmt.Println()

	if err := http.ListenAndServe(address, router); err != nil {
		log.Fatal(err)
	}
}

func auth(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authToken != "" {
			if authToken != r.Header.Get("Authorization") {
				http.Error(w, "authorization header missing", http.StatusBadRequest)
				return
			}
		}
		h.ServeHTTP(w, r)
	})
}

func getS3Client(endpoint string, accessKey string, secretKey string, insecure bool) (*minio.Client, error) {
	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	secure := strings.EqualFold(u.Scheme, "https")
	transport, err := minio.DefaultTransport(secure)
	if err != nil {
		return nil, err
	}
	if transport.TLSClientConfig != nil {
		transport.TLSClientConfig.InsecureSkipVerify = insecure
	}
	s3Client, err := minio.New(u.Host, &minio.Options{
		Creds:     credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure:    secure,
		Transport: transport,
	})
	if err != nil {
		return nil, err
	}
	return s3Client, nil
}
