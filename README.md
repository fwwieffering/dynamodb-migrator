# DynamoDB Migrator
A utility for migrating data from one dynamodb table to another. Dynamodb does not have any native utility for this.

## Usage
- source table is configured by setting the `SOURCE_TABLE` environment variable
- destination table is configured by setting the `DESTINATION_TABLE` environment variable

Running:
```
go install
dynamodb-migrator
```

## A couple things to consider
- This does no transformation of the data. Indexes / Attributes must be the same across tables
- The program assumes whatever IAM credentials are provided to the environment it runs in. It will need `dynamodb:Scan` permissions on the source table and `dynamodb:BatchWriteItem, dynamodb:PutItem` permissions on the destination table
- There is very little error handling. If you hit a provisioned throughput error, it will fail. AWS automatically retries this but depending on the amount of data you are dealing with you may need to increase read/write capacity on your tables (or implement autoscaling of your dynamo tables)
- You can run this multiple times but items will not be updated as `BatchWriteItem` does not do updates
