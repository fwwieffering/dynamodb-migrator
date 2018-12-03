package main

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	log "github.com/sirupsen/logrus"
)

// Table provides the capability to read and write to tables
type Table struct {
	Name      string
	Client    dynamodbiface.DynamoDBAPI
	ItemCount int
	ItemChan  chan []map[string]*dynamodb.AttributeValue
}

// NewTable creates table struct and returns it
func NewTable(client dynamodbiface.DynamoDBAPI, name string) *Table {
	return &Table{
		Name:      name,
		Client:    client,
		ItemCount: 0,
		ItemChan:  make(chan []map[string]*dynamodb.AttributeValue),
	}
}

// PullItems sends off async scans of dynamodb table and writes the items to a channel
func (t *Table) PullItems(waitChan chan byte) {
	// set table item count to 0, will be refreshed after items are pulled
	t.ItemCount = 0
	lockChan := make(chan byte)
	// dynamodb scanners
	shards := 5

	for i := 0; i < shards; i++ {
		go t.ScanTable(i, shards, lockChan)
	}
	cleanUpThreads("PullItems", shards, lockChan)
	close(t.ItemChan)
	log.Println("Item channel closed")
	waitChan <- 1
}

// ScanTable is executes a parallel scan on the dynamo database
// all items are appended to a channel.
func (t *Table) ScanTable(shard int, totalShards int, lockChan chan byte) {
	counter := 1

	res, err := handleScanProvisionedThroughput(t.Client, &dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		Select:         aws.String("ALL_ATTRIBUTES"),
		TableName:      &t.Name,
		Segment:        aws.Int64(int64(shard)),
		TotalSegments:  aws.Int64(int64(totalShards)),
	})

	for len(res.LastEvaluatedKey) > 0 && err == nil {
		log.Printf("Shard %d pulled page %d from %s", shard, counter, t.Name)
		counter++
		t.ItemCount = t.ItemCount + len(res.Items)
		makeBatches(t.ItemChan, res.Items)

		res, err = handleScanProvisionedThroughput(t.Client, &dynamodb.ScanInput{
			ConsistentRead:    aws.Bool(true),
			Select:            aws.String("ALL_ATTRIBUTES"),
			TableName:         &t.Name,
			ExclusiveStartKey: res.LastEvaluatedKey,
			Segment:           aws.Int64(int64(shard)),
			TotalSegments:     aws.Int64(int64(totalShards)),
		})
	}
	if err != nil {
		panic(err)
	}

	t.ItemCount = t.ItemCount + len(res.Items)
	makeBatches(t.ItemChan, res.Items)

	log.Printf("Shard %d finished pulling items from %s", shard, t.Name)
	lockChan <- 1
}

// PushItems creates many goroutines to write items to DynamoDB
// Items are written through the BatchWriteItem method and unprocessed items are retried three times
// the number of concurrent threads is limited by the maxThreads variable
func (t *Table) PushItems(itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
	lockChan := make(chan byte)
	goroCount := 0
	counter := 0
	unprocessedItems := make(chan map[string]*dynamodb.AttributeValue, 100000)

	// limit concurrency through use of buffered channel
	maxThreads := 30
	concurrentThreads := make(chan bool, maxThreads)

	for i := 0; i < maxThreads; i++ {
		concurrentThreads <- true
	}

	for itemList := range itemChan {
		goroCount++
		counter = counter + len(itemList)
		<-concurrentThreads
		log.Printf("Thread %d/%d Dispatched items %d-%d for processing", maxThreads-len(concurrentThreads), maxThreads, counter-len(itemList), counter)
		go t.BatchWriteItems(itemList, concurrentThreads, lockChan, unprocessedItems)
	}
	cleanUpThreads("PushItems", goroCount, lockChan)
	close(unprocessedItems)
	log.Printf("Cleaning up %d unprocessed items", len(unprocessedItems))
	counter = 0
	for i := range unprocessedItems {
		counter++
		log.Printf("Storing unprocessed item %d", counter)
		_, err := t.Client.PutItem(&dynamodb.PutItemInput{
			TableName: &t.Name,
			Item:      i,
		})
		if err != nil {
			log.Printf("%+v", err)
		}
	}
	waitChan <- 1
}

// BatchWriteItems executes an aws batchWriteItems command and handles the unprocessed items
// Unprocessed items are retried 3 times and then appended to a channel to be processed later
func (t *Table) BatchWriteItems(items []map[string]*dynamodb.AttributeValue, concurrentThreads chan bool, lockChan chan byte, unprocessedItems chan map[string]*dynamodb.AttributeValue) {
	input, _ := generateBatchWriteInput(t.Name, items)
	res, batchErr := handleWriteProvisionedThroughput(t.Client, input)
	if batchErr != nil {
		panic(batchErr)
	}
	// process unprocessed items
	// retries 3 times
	unprocessed := res.UnprocessedItems
	retries := 0
	for len(unprocessed) > 0 && retries < 3 {
		log.Infoln(fmt.Sprintf("%d unprocessed items. Processing...", len(unprocessed)))
		res, err := handleWriteProvisionedThroughput(t.Client, &dynamodb.BatchWriteItemInput{
			RequestItems: unprocessed,
		})
		unprocessed = res.UnprocessedItems
		retries++
		if err != nil {
			panic(err)
		}
		// try not to overload tables
		time.Sleep(1 * time.Second)
	}
	if retries == 3 && len(unprocessed) > 0 {
		log.Errorln(fmt.Sprintf("Unable to process %d items. Writing them to the cleanup channel", len(unprocessed)))
		for u := range unprocessed[t.Name] {
			log.Infoln(fmt.Sprintf("Length of unprocessed items channel: %d", len(unprocessedItems)))
			unprocessedItems <- unprocessed[t.Name][u].PutRequest.Item
		}
	}
	// free up room for another thread
	concurrentThreads <- true
	// for syncing
	lockChan <- 1
}
