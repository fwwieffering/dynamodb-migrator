# DynamoDB Migrator
A utility for migrating data from one dynamodb table to another. Dynamodb does not have any native utility for this.

## Usage
- source table is configured by setting the `SOURCE_TABLE` environment variable
- destination table is configured by setting the `DESTINATION_TABLE` environment variable
- region is configured by the `AWS_REGION` environment variable. Default is us-east-1

Optionally, you can set up cross account migrations by providing the following env vars
- `SOURCE_ACCOUNT`: the account number of the source account
- `DESTINATION_ACCOUNT`: the account number of the destination account
- `CROSS_ACCOUNT_ROLE`: the name of a role with dynamo permissions that exists in both accounts.

Running:
```
go install
dynamodb-migrator
```


## A couple things to consider
- This does no transformation of the data. Indexes / Attributes must be the same across tables
- The program assumes whatever IAM credentials are provided to the environment it runs in. It will need `dynamodb:Scan` permissions on the source table, `dynamodb:BatchWriteItem, dynamodb:PutItem` permissions on the destination table, and `dynamodb:UpdateTable` permissions on each table
- The script increases the read/write capacity of the tables and all indexes to 3000, then knocks it back down to the original settings after running. Allocating all this write can take a while and the table will be expensive while it has this write capacity
- You can run this multiple times but items will not be updated as `BatchWriteItem` does not do updates
