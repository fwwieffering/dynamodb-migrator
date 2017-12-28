package main

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func TestMakeBatches(t *testing.T) {
	// some non divisible by 15 value
	testList := make([]map[string]*dynamodb.AttributeValue, 32)
	testChan := make(chan []map[string]*dynamodb.AttributeValue, 3)

	makeBatches(testChan, testList)
	close(testChan)
	batch1 := <-testChan
	if len(batch1) != 15 {
		t.Fatalf("makeBatches should make a batch size of 15. Length of batch1: %d", len(batch1))
	}
	batch2 := <-testChan
	if len(batch2) != 15 {
		t.Fatalf("makeBatches should make a batch size of 15. Length of batch2: %d", len(batch1))
	}
	batch3 := <-testChan
	if len(batch3) != 2 {
		t.Fatalf("Last batch should be 2 (15, 15, 2)")
	}
}

func TestCleanupThreads(t *testing.T) {
	testChan := make(chan byte, 2)
	testChan <- 1
	testChan <- 1
	cleanUpThreads("test", 2, testChan)
	if len(testChan) != 0 {
		t.Fatal("cleanUpThreads should empty out the channel")
	}
}

func TestGenBatchWriteError(t *testing.T) {
	testItems := make([]map[string]*dynamodb.AttributeValue, 28)
	_, err := generateBatchWriteInput("failTest", testItems)
	if err == nil {
		t.Fatal("GenerateBatchWriteInput should return an error when item length > 25")
	}
}

func TestGenBatchWriteSuccess(t *testing.T) {
	testItems := make([]map[string]*dynamodb.AttributeValue, 5)
	_, err := generateBatchWriteInput("succeedTest", testItems)
	if err != nil {
		t.Fatal("GenerateBatchWriteInput should suceed when item length < 25")
	}
}
