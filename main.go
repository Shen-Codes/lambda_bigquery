package main

import (
	"context"
	"io/ioutil"
	"log"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

//Dataset copied from dataset struct

// var bigClient *bigquery.Client
var client *bigquery.Client
var ctx context.Context

func init() {
	svc := s3.New(session.New())
	input := &s3.GetObjectInput{
		Bucket: aws.String("jsonfiles312021"),
		Key:    aws.String("Project-8a8c500b8c6d.json"),
	}
	result, _ := svc.GetObject(input)
	defer result.Body.Close()
	body, _ := ioutil.ReadAll(result.Body)

	ctx = context.Background()

	var err error
	client, err = bigquery.NewClient(ctx, "first-vision-305321", option.WithCredentialsJSON(body))
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}

}

func main() {
	lambda.Start(handler)
}

func handler() ([]string, error) {
	it, err := datasets(ctx, client)
	if err != nil {
		log.Fatal(err)
	}
	datasets, _ := SliceResults(it)
	return datasets, nil
}

//get dataset interator from BigQuery
func datasets(ctx context.Context, client *bigquery.Client) (*bigquery.DatasetIterator, error) {
	it := client.Datasets(ctx)
	return it, nil
}

//SliceResults returns slice of strings of list of data sets
func SliceResults(iter *bigquery.DatasetIterator) ([]string, error) {
	var datasets []string
	for {
		dataset, err := iter.Next()

		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		datasets = append(datasets, dataset.DatasetID)
	}
	return datasets, nil
}
