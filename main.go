package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	// Optimal routine pool size
	const MAX_ROUTINES = 200

	var bucketName = ""
	var accountId = ""
	var accessKeyId = ""
	var accessKeySecret = ""

	r2Resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountId),
		}, nil
	})

	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithEndpointResolverWithOptions(r2Resolver),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyId, accessKeySecret, "")),
	)
	if err != nil {
		log.Fatal(err)
	}
	client := s3.NewFromConfig(cfg)

	uploadChan := make(chan *s3.PutObjectInput, MAX_ROUTINES)

	runUploader := func(wg *sync.WaitGroup) {
		for obj := range uploadChan {
			_, err := client.PutObject(context.Background(), obj)
			if err != nil {
				log.Fatal(err)
				return
			}
			fmt.Printf("uploaded %s\n", *obj.Key)
			wg.Done()
		}
	}

	count := 0
	start := time.Now()
	var wgUpload sync.WaitGroup
	go runUploader(&wgUpload)

	uploaders := []func(){}
	for i := 0; i < MAX_ROUTINES; i++ {
		uploaders = append(uploaders, func() { runUploader(&wgUpload) })
	}

	for _, uploader := range uploaders {
		go uploader()
	}

	objectsToUpload := iterateForUpload("sample", "schachte")
	for _, objectToRemove := range objectsToUpload {
		putReq := &s3.PutObjectInput{
			Bucket: &bucketName,
			Key:    objectToRemove.Key,
		}
		wgUpload.Add(1)
		uploadChan <- putReq
		count++
	}
	wgUpload.Wait()
	fmt.Printf("Uploaded %d files [concurrently] in %.2f seconds\n", count, time.Since(start).Seconds())

	deletionChan := make(chan *s3.DeleteObjectInput, MAX_ROUTINES)
	runDeleter := func(wg *sync.WaitGroup) {
		for obj := range deletionChan {
			_, err := client.DeleteObject(context.Background(), obj)
			if err != nil {
				log.Fatal(err)
				return
			}
			fmt.Printf("deleted %s\n", *obj.Key)
			wg.Done()
		}
	}

	getObjects := func() *s3.ListObjectsOutput {
		resp, err := client.ListObjects(context.Background(), &s3.ListObjectsInput{
			Bucket: aws.String(bucketName),
		})
		if err != nil {
			log.Fatal("listing buckets")
		}
		return resp
	}

	count = 0
	start = time.Now()
	var wgDelete sync.WaitGroup
	deleters := []func(){}
	for i := 0; i < MAX_ROUTINES; i++ {
		deleters = append(deleters, func() { runDeleter(&wgDelete) })
	}

	for _, deleter := range deleters {
		go deleter()
	}

	for {
		obj := getObjects().Contents
		size := len(obj)
		if size == 0 {
			return
		}
		for _, objectToRemove := range obj {
			deleteReq := &s3.DeleteObjectInput{
				Bucket: &bucketName,
				Key:    objectToRemove.Key,
			}
			wgDelete.Add(1)
			deletionChan <- deleteReq
			count++
		}
		wgDelete.Wait()
		fmt.Printf("Deleted %d files [concurrently] in %.2f seconds\n", count, time.Since(start).Seconds())
	}

}

func iterateForUpload(inputPath, bucketName string) []*s3.PutObjectInput {
	objects := []*s3.PutObjectInput{}
	filepath.Walk(inputPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalf(err.Error())
		}
		file, err := os.Open(path)
		if err != nil {
			log.Fatalf(err.Error())
		}

		fileInfo, _ := file.Stat()
		size := fileInfo.Size()
		buffer := make([]byte, size)
		file.Read(buffer)
		fileBytes := bytes.NewReader(buffer)

		objects = append(objects, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(info.Name()),
			Body:   fileBytes,
		})
		return nil
	})
	return objects
}
