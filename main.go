package main

import (
	"context"
	"encoding/json"
	"log"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ssm"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

//client initialized globally to be accessed by other functions
var client *bigquery.Client
var ctx context.Context

//Lambda accepts init functions and runs them like a regular Go program https://docs.aws.amazon.com/lfilesambda/latest/dg/lambda-golang.html
func init() {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
		Config: aws.Config{
			Region: aws.String("us-east-1"),
		},
	}))

	svc := ssm.New(sess)

	paramsFromAWS := paramsByPath(svc)
	paramsByte, _ := json.Marshal(paramsFromAWS)
	ctx = context.Background()

	var err error
	client, err = bigquery.NewClient(ctx, "first-vision-305321", option.WithCredentialsJSON(paramsByte))
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}

}

func paramsByPath(svc *ssm.SSM) map[string]string {
	pathInput := &ssm.GetParametersByPathInput{
		Path: aws.String("/bqconfig"),
	}

	res, err := svc.GetParametersByPath(pathInput)
	if err != nil {
		log.Println(err)
	}

	params := make(map[string]string)

	for _, param := range res.Parameters {
		name := strings.Replace(*param.Name, "/bqconfig/", "", -1)
		value := *param.Value
		params[name] = value
	}

	return params
}

func main() {
	lambda.Start(handler)
}

//Lambda functions must have a handler that is called in the main function. See docs for details https://docs.aws.amazon.com/lambda/latest/dg/lambda-golang.html
func handler() ([]string, error) {
	it, err := datasets(ctx, client)
	if err != nil {
		log.Fatal(err)
	}
	datasets, _ := SliceResults(it)
	return datasets, nil
}

//get dataset interator from BigQuery https://pkg.go.dev/cloud.google.com/go/bigquery#Client.Datasets
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
