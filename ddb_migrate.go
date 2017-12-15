package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Checks if DescribeTableOutput is active or not
func updateTable(client *dynamodb.DynamoDB, table string, input *dynamodb.UpdateTableInput) {
	_, err := client.UpdateTable(input)
	if err != nil {
		if strings.Contains(err.Error(), "will not change") {
			log.Printf("No updates to be made")
		} else if strings.Contains(err.Error(), "is being updated") {
			log.Printf("Update already in progress")
		} else {
			panic(err)
		}
	}

	// wait for table update to complete
	updateRes, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &table,
	})

	// parses describetable output
	getStatus := func(output *dynamodb.DescribeTableOutput) bool {
		active := true
		if *output.Table.TableStatus != "ACTIVE" {
			active = false
			return active
		}
		for i := range output.Table.GlobalSecondaryIndexes {
			if *output.Table.GlobalSecondaryIndexes[i].IndexStatus != "ACTIVE" {
				active = false
				return active
			}
		}
		return active
	}

	for !getStatus(updateRes) {
		if err != nil {
			panic(err)
		}
		log.Printf("Waiting for table update to complete....")
		updateRes, err = client.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: &table,
		})
		time.Sleep(5 * time.Second)
	}
	log.Printf("Table update complete. Table in status %s", *updateRes.Table.TableStatus)
}

// updateCapacity increases the read/write capacity of a table and all GlobalSecondaryIndexes
// 3000 read/writes is probably too many for most cases but should give runway for large tables
func increaseTableCapacity(table string, readOrWrite string, client *dynamodb.DynamoDB) *dynamodb.UpdateTableInput {
	// Get table throughput information
	res, err := client.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: &table,
	})
	if err != nil {
		panic(err)
	}
	var readCapacity *int64
	var writeCapacity *int64

	if readOrWrite == "write" {
		readCapacity = res.Table.ProvisionedThroughput.ReadCapacityUnits
		writeCapacity = aws.Int64(3000)
	} else {
		readCapacity = aws.Int64(3000)
		writeCapacity = res.Table.ProvisionedThroughput.WriteCapacityUnits
	}
	originalSettings := dynamodb.UpdateTableInput{
		TableName: &table,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  res.Table.ProvisionedThroughput.ReadCapacityUnits,
			WriteCapacityUnits: res.Table.ProvisionedThroughput.WriteCapacityUnits,
		},
	}
	newSettings := dynamodb.UpdateTableInput{
		TableName: &table,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			WriteCapacityUnits: writeCapacity,
			ReadCapacityUnits:  readCapacity,
		},
	}
	// Collect global secondary indexes and increase capacity as well
	for i := range res.Table.GlobalSecondaryIndexes {
		if readOrWrite == "write" {
			readCapacity = res.Table.ProvisionedThroughput.ReadCapacityUnits
			writeCapacity = aws.Int64(3000)
		} else {
			readCapacity = aws.Int64(3000)
			writeCapacity = res.Table.ProvisionedThroughput.WriteCapacityUnits
		}
		if res.Table.GlobalSecondaryIndexes[i].ProvisionedThroughput.WriteCapacityUnits != writeCapacity || res.Table.GlobalSecondaryIndexes[i].ProvisionedThroughput.ReadCapacityUnits != readCapacity {
			newSettings.GlobalSecondaryIndexUpdates = append(newSettings.GlobalSecondaryIndexUpdates, &dynamodb.GlobalSecondaryIndexUpdate{
				Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
					IndexName: res.Table.GlobalSecondaryIndexes[i].IndexName,
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						WriteCapacityUnits: writeCapacity,
						ReadCapacityUnits:  readCapacity,
					},
				},
			})
			originalSettings.GlobalSecondaryIndexUpdates = append(newSettings.GlobalSecondaryIndexUpdates, &dynamodb.GlobalSecondaryIndexUpdate{
				Update: &dynamodb.UpdateGlobalSecondaryIndexAction{
					IndexName: res.Table.GlobalSecondaryIndexes[i].IndexName,
					ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
						WriteCapacityUnits: res.Table.GlobalSecondaryIndexes[i].ProvisionedThroughput.WriteCapacityUnits,
						ReadCapacityUnits:  res.Table.GlobalSecondaryIndexes[i].ProvisionedThroughput.ReadCapacityUnits,
					},
				},
			})
		}
	}

	updateTable(client, table, &newSettings)

	return &originalSettings
}

// pullItems sends off async scans of dynamodb table and writes the items to a channel
func pullItems(client *dynamodb.DynamoDB, srcTable string, itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
	// paginate
	lockChan := make(chan byte)
	// dynamodb scanners
	shards := 5

	for i := 0; i < shards; i++ {
		go scanTable(client, srcTable, i, shards, itemChan, lockChan)
	}
	cleanUpThreads("PullItems", shards, lockChan)
	close(itemChan)
	log.Println("Item channel closed")
	waitChan <- 1
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

// scanTable is executes a parallel scan on the dynamo database
func scanTable(client *dynamodb.DynamoDB, table string, shard int, totalShards int, itemChan chan []map[string]*dynamodb.AttributeValue, lockChan chan byte) {
	counter := 1
	itemCount := 0

	res, err := client.Scan(&dynamodb.ScanInput{
		ConsistentRead: aws.Bool(true),
		Select:         aws.String("ALL_ATTRIBUTES"),
		TableName:      &table,
		Segment:        aws.Int64(int64(shard)),
		TotalSegments:  aws.Int64(int64(totalShards)),
	})

	itemCount = itemCount + len(res.Items)
	makeBatches(itemChan, res.Items)

	for len(res.LastEvaluatedKey) > 0 && err == nil {
		log.Printf("Shard %d pulled page %d from %s", shard, counter, table)
		counter++
		itemCount = itemCount + len(res.Items)
		makeBatches(itemChan, res.Items)

		res, err = client.Scan(&dynamodb.ScanInput{
			ConsistentRead:    aws.Bool(true),
			Select:            aws.String("ALL_ATTRIBUTES"),
			TableName:         &table,
			ExclusiveStartKey: res.LastEvaluatedKey,
			Segment:           aws.Int64(int64(shard)),
			TotalSegments:     aws.Int64(int64(totalShards)),
		})
	}
	if err != nil {
		panic(err)
	}
	log.Printf("Shard %d finished pulled %d items from %s", shard, itemCount, table)
	lockChan <- 1
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

// batchWriteItems executes an aws batchWriteItems command and handles the unprocessed items
func batchWriteItems(client *dynamodb.DynamoDB, table string, items []map[string]*dynamodb.AttributeValue, concurrentThreads chan bool, lockChan chan byte, unprocessedItems chan map[string]*dynamodb.AttributeValue) {
	input, err := generateBatchWriteInput(table, items)
	if err != nil {
		panic(err)
	}
	res, batchErr := client.BatchWriteItem(input)
	if batchErr != nil {
		panic(batchErr)
	}
	// process unprocessed items
	// retries 3 times
	unprocessed := res.UnprocessedItems
	retries := 0
	for len(unprocessed) > 0 && retries < 3 {
		log.Printf("%d unprocessed items. Processing...", len(unprocessed))
		res, err = client.BatchWriteItem(&dynamodb.BatchWriteItemInput{
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
		log.Printf("Unable to process %d items", len(unprocessed))
		for u := range unprocessed[table] {
			log.Printf("Length of unprocessed items channel: %d", len(unprocessedItems))
			unprocessedItems <- unprocessed[table][u].PutRequest.Item
		}
	}
	// free up room for another thread
	concurrentThreads <- true
	// for syncing
	lockChan <- 1
}

// pushItems creates many goroutines to write items to DynamoDB
// the number of concurrent threads is limited by the maxThreads variable
func pushItems(client *dynamodb.DynamoDB, destTable string, itemChan chan []map[string]*dynamodb.AttributeValue, waitChan chan byte) {
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
		go batchWriteItems(client, destTable, itemList, concurrentThreads, lockChan, unprocessedItems)
	}
	cleanUpThreads("PushItems", goroCount, lockChan)
	close(unprocessedItems)
	log.Printf("Cleaning up %d unprocessed items", len(unprocessedItems))
	counter = 0
	for i := range unprocessedItems {
		counter++
		log.Printf("Storing unprocessed item %d", counter)
		_, err := client.PutItem(&dynamodb.PutItemInput{
			TableName: &destTable,
			Item:      i,
		})
		if err != nil {
			log.Printf("%+v", err)
		}
	}
	waitChan <- 1
}

// cleanUpThreads is a syncing tool
func cleanUpThreads(source string, count int, channel chan byte) {
	log.Printf("Cleaning up %d threads for %s", count, source)
	for i := 0; i < count; i++ {
		log.Printf("Cleaned up thread %d of %d for %s", i+1, count, source)
		<-channel
	}
}

// returns dynamodb clients for source/dest tables
// if SOURCE_ACCOUNT, DESTINATION_ACCOUNT, CROSS_ACCOUNT_ROLE env vars are provided
// clients will point to different accounts. Otherwise default credentials are used
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

func main() {
	sourceTable, present := os.LookupEnv("SOURCE_TABLE")
	if !present {
		panic("SOURCE_TABLE environment variable must be defined")
	}
	destTable, present := os.LookupEnv("DESTINATION_TABLE")
	if !present {
		panic("DESTINATION_TABLE environment variable must be defined")
	}
	region, present := os.LookupEnv("AWS_REGION")
	if !present {
		region = "us-east-1"
	}
	log.Printf("Dynamodb migration from %s to %s", sourceTable, destTable)
	itemChan := make(chan []map[string]*dynamodb.AttributeValue)
	waitChan := make(chan byte, 2)
	srcDdb, destDdb := generateClients(region)
	log.Printf("Increasing table %s write capacity to 3000 for table and all GSIs", destTable)
	destOrigSettings := increaseTableCapacity(destTable, "write", destDdb)
	log.Printf("Increasing table %s read capacity to 3000 for table and all GSIs", sourceTable)
	srcOrigSettings := increaseTableCapacity(sourceTable, "read", srcDdb)
	log.Println("Pulling items from source table")
	go pullItems(srcDdb, sourceTable, itemChan, waitChan)
	go pushItems(destDdb, destTable, itemChan, waitChan)
	<-waitChan
	<-waitChan
	log.Printf("Knocking table %s write capacity back down to original settings", destTable)
	updateTable(destDdb, destTable, destOrigSettings)
	log.Printf("Knocking table %s read capacity back down to original settings", sourceTable)
	updateTable(srcDdb, sourceTable, srcOrigSettings)

}
