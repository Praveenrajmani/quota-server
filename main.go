package main

import (
	"flag"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/gorilla/mux"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/pkg/env"
)

var (
	address   string
	authToken = env.Get("WEBHOOK_AUTH_TOKEN", "")
	endpoint  = env.Get("MINIO_ENDPOINT", "")
	accessKey = env.Get("MINIO_ACCESS_KEY", "")
	secretKey = env.Get("MINIO_SECRET_KEY", "")
	insecure  = env.Get("MINIO_INSECURE", "false") == "true"
	s3Client  *minio.Client
	dryRun    bool
)

func main() {
	flag.StringVar(&address, "address", ":8080", "bind to a specific ADDRESS:PORT, ADDRESS can be an IP or hostname")
	flag.BoolVar(&dryRun, "dry-run", false, "Enable dry run mode")
	flag.Parse()

	if endpoint == "" {
		log.Fatal("MINIO_ENDPOINT env is not set")
	}
	if accessKey == "" {
		log.Fatal("MINIO_ACCESS_KEY env is not set")
	}
	if secretKey == "" {
		log.Fatal("MINIO_SECRET_KEY env is not set")
	}

	var err error
	s3Client, err = getS3Client(endpoint, accessKey, secretKey, insecure)
	if err != nil {
		log.Fatalf("unable to create s3 client; %v", err)
	}

	router := mux.NewRouter()

	router.Handle("/quota/update", auth(http.HandlerFunc(updateQuotaHandler))).Methods("POST")
	router.Handle("/quota/check/{user}", auth(http.HandlerFunc(quotaCheckHandler))).Methods("GET")
	router.Handle("/quota/refresh", auth(http.HandlerFunc(quotaRefreshHandler)))

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
