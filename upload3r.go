package main

import (
	"flag"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type s3payload struct {
	concurrency  int
	endpoint     string
	region       string
	keyId        string
	secret       string
	permissions  string
	source       string
	bucket       string
	bucketPrefix string
}

func main() {
	endpoint := flag.String("uri", "https://hb.bizmrg.com", "AWS S3 Endpoint")
	region := flag.String("region", "ru-msk", "AWS S3 Region")
	keyId := flag.String("key-id", "", "AWS KEY ID")
	secret := flag.String("secret", "", "AWS SECRET ACCESS KEY")
	permissions := flag.String("permissions", "private", "Access permissions for uploaded source")
	source := flag.String("source", "", "Source for upload (file or directory)")
	bucket := flag.String("bucket", "", "AWS S3 bucket")
	bucketPrefix := flag.String("bucket-prefix", "", "AWS S3 bucket prefix")
	concurrency := flag.Int("thread-num", 10, "Num of worker threads")
	flag.Parse()

	payload := s3payload{
		endpoint:     *endpoint,
		region:       *region,
		keyId:        *keyId,
		secret:       *secret,
		permissions:  *permissions,
		source:       *source,
		bucket:       *bucket,
		bucketPrefix: *bucketPrefix,
		concurrency:  *concurrency,
	}

	log.Print("AWS S3 Uploader started!")

	if isDirectory(*source) {
		uploadDirToS3(payload)
	} else {
		uploadFileToS3(payload)
	}

	log.Print("AWS S3 Uploader finished successfully!")
}

func uploadDirToS3(payload s3payload) {
	dirPath, _ := filepath.Abs(payload.source)
	var files []string
	err := filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if isDirectory(path) {
			return nil
		} else {
			files = append(files, path)
			return nil
		}
	})
	checkErr(err)

	var wg sync.WaitGroup
	sem := make(chan bool, payload.concurrency)
	for _, file := range files {
		dir := filepath.Dir(file)
		prefix := payload.bucketPrefix + strings.Replace(dir, dirPath, "", -1)
		prefix = strings.Replace(prefix, "\\", "/", -1)

		wg.Add(1)
		sem <- true
		go func(endpoint string, region string, keyId string, secret string, permissions string, bucket string, bucketPrefix string, filePath string) {
			defer func() { <-sem }()
			defer wg.Done()
			filePayload := s3payload{
				endpoint:     endpoint,
				region:       region,
				keyId:        keyId,
				secret:       secret,
				permissions:  permissions,
				source:       filePath,
				bucket:       bucket,
				bucketPrefix: bucketPrefix,
			}
			uploadFileToS3(filePayload)
		}(payload.endpoint, payload.region, payload.keyId, payload.secret, payload.permissions, payload.bucket, prefix, file)
	}

	for i := 0; i < cap(sem); i++ {
		sem <- true
	}

	wg.Wait()
}

func uploadFileToS3(payload s3payload) {
	fileName := filepath.Base(payload.source)
	fileKey := payload.bucketPrefix + "/" + fileName
	log.Println("Upload " + payload.source + " to " + payload.bucket + "/" + fileKey)

	s3Svc := s3.New(createSession(payload.endpoint, payload.region, payload.keyId, payload.secret))
	file, err := os.Open(payload.source)
	checkErr(err)
	defer file.Close()

	data := &s3.PutObjectInput{
		Bucket: aws.String(payload.bucket),
		Key:    aws.String(fileKey),
		Body:   file,
		ACL:    aws.String(payload.permissions),
	}

	_, err = s3Svc.PutObject(data)
	checkErr(err)
}

func createSession(endpoint string, region string, keyId string, secret string) *session.Session {
	sess, err := session.NewSession(&aws.Config{
		Endpoint:    aws.String(endpoint),
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(keyId, secret, ""),
	})
	checkErr(err)

	return sess
}

func isDirectory(path string) bool {
	fd, err := os.Stat(path)
	checkErr(err)

	switch mode := fd.Mode(); {
	case mode.IsDir():
		return true
	case mode.IsRegular():
		return false
	}
	return false
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
		os.Exit(-666)
	}
}
