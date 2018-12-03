package main

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	log "github.com/sirupsen/logrus"
)

// generateClient returns dynamodb client using whatever credentials are currently
// configured.
func generateClient(region string) *dynamodb.DynamoDB {
	var client *dynamodb.DynamoDB
	// log.Printf("Using default credentials")
	client = dynamodb.New(session.New(&aws.Config{Region: &region}))
	return client
}

// generateClientRole returns dynamodb client by assuming the role in the given account
func generateClientRole(region string, account string, role string) *dynamodb.DynamoDB {
	roleArn := fmt.Sprintf("arn:aws:iam::%s:role/%s", account, role)
	sess := session.Must(session.NewSession(&aws.Config{Region: &region}))
	client := dynamodb.New(sess, &aws.Config{Credentials: stscreds.NewCredentials(sess, roleArn)})
	return client
}

// generateBatchWriteInput outputs the input for an aws BatchWriteItem command
func generateBatchWriteInput(table string, items []map[string]*dynamodb.AttributeValue) (*dynamodb.BatchWriteItemInput, error) {
	requestItems := make(map[string][]*dynamodb.WriteRequest)
	if len(items) > 25 {
		return nil, errors.New("Can not create batch write request > 25 items")
	}
	for i := range items {
		requestItems[table] = append(requestItems[table], &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: items[i],
			},
		})
	}
	return &dynamodb.BatchWriteItemInput{RequestItems: requestItems}, nil
}

// makeBatches creates smaller arrays out of large arrays and appends to a channel
func makeBatches(itemChan chan []map[string]*dynamodb.AttributeValue, itemList []map[string]*dynamodb.AttributeValue) {
	batchSize := 15
	lastBatch := 0
	for item := range itemList {
		if item%batchSize == 0 && item > 0 {
			itemChan <- itemList[item-batchSize : item]
			lastBatch = item
		}
	}
	if lastBatch != len(itemList) {
		itemChan <- itemList[lastBatch:]
	}
}

// cleanUpThreads is a syncing tool
func cleanUpThreads(source string, count int, channel chan byte) {
	log.Printf("Cleaning up %d threads for %s", count, source)
	for i := 0; i < count; i++ {
		log.Infoln(fmt.Sprintf("Cleaned up thread %d of %d for %s", i+1, count, source))
		<-channel
	}
}

func isProvisionedThroughputException(dynamoErr error) bool {
	if aerr, ok := dynamoErr.(awserr.Error); ok {
		return aerr.Code() == dynamodb.ErrCodeProvisionedThroughputExceededException
	}
	return false
}

// handleScanProvisionedThroughput retries Scan calls on provisionedThroughput until successful
func handleScanProvisionedThroughput(client dynamodbiface.DynamoDBAPI, scanInput *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	res, err := client.Scan(scanInput)

	if err != nil && isProvisionedThroughputException(err) {
		for isProvisionedThroughputException(err) {
			log.Infoln("ProvisionedThroughputExceeded exception for Scan. Retrying in 5 secs")
			time.Sleep(5 * time.Second)
			res, err = client.Scan(scanInput)
		}
	}
	return res, err
}

// handleWriteProvisionedThroughput retries BatchWrite calls that return ProvisionedThroughputExceeded exceptions until successful
func handleWriteProvisionedThroughput(client dynamodbiface.DynamoDBAPI, writeInput *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
	res, err := client.BatchWriteItem(writeInput)

	if err != nil && isProvisionedThroughputException(err) {
		for isProvisionedThroughputException(err) {
			log.Infoln("ProvisionedThroughputExceeded exception for Write. Retrying in 5 secs")
			time.Sleep(5 * time.Second)
			res, err = client.BatchWriteItem(writeInput)
		}
	}
	return res, err
}

// ItemCount a struct for passing Item Counts between different threads
type ItemCount struct {
	Lock  sync.Mutex // <-- this mutex protects
	Count int        // <-- this integer underneath
}

// NewItemCount generates an ItemCount struct at count 0
func NewItemCount() *ItemCount {
	return &ItemCount{
		Lock:  sync.Mutex{},
		Count: 0,
	}
}
