package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// returns dynamodb clients for source/dest tables
// if SOURCE_ACCOUNT, DESTINATION_ACCOUNT, CROSS_ACCOUNT_ROLE env vars are provided
// clients will point to different accounts. Otherwise default credentials are used
// not sure how to write a unit test for this
func generateClients(region string) (*dynamodb.DynamoDB, *dynamodb.DynamoDB) {
	srcAcct, srcAcctPresent := os.LookupEnv("SOURCE_ACCOUNT")
	destAcct, destAcctPresent := os.LookupEnv("DESTINATION_ACCOUNT")
	role, rolePresent := os.LookupEnv("CROSS_ACCOUNT_ROLE")

	var srcClient *dynamodb.DynamoDB
	var destClient *dynamodb.DynamoDB

	if rolePresent && destAcctPresent && srcAcctPresent {
		log.Printf("Assuming cross account role %s", role)
		srcRole := fmt.Sprintf("arn:aws:iam::%s:role/%s", srcAcct, role)
		destRole := fmt.Sprintf("arn:aws:iam::%s:role/%s", destAcct, role)
		sess := session.Must(session.NewSession(&aws.Config{Region: &region}))
		srcClient = dynamodb.New(sess, &aws.Config{Credentials: stscreds.NewCredentials(sess, srcRole)})
		destClient = dynamodb.New(sess, &aws.Config{Credentials: stscreds.NewCredentials(sess, destRole)})
	} else {
		log.Printf("Using default credentials")
		srcClient = dynamodb.New(session.New(&aws.Config{Region: &region}))
		destClient = dynamodb.New(session.New(&aws.Config{Region: &region}))
	}
	return srcClient, destClient
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
		log.Printf("Cleaned up thread %d of %d for %s", i+1, count, source)
		<-channel
	}
}
