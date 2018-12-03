# DynamoDB Migrator
A utility for migrating data from one dynamodb table to another. Dynamodb does not have any native utility for this.

## Usage
commands:
- migrate: writes items from one dynamo table to another
```
$ dynamodb-migrator migrate --help
NAME:
   dynamodb-migrator migrate - Migrates data from Source Table to Destination Table

USAGE:
   dynamodb-migrator migrate [command options] [arguments...]

OPTIONS:
   --source-table value, --st value         Required: Dynamodb Table Name to source data from [$SOURCE_TABLE]
   --source-region value, --sr value        aws region of source table (default: "us-east-1") [$SOURCE_REGION]
   --source-account value, --sa value       AWS Account number of the source table [$SOURCE_ACCOUNT]
   --destination-table value, --dt value    Required: Dynamodb Table Name to write to [$DESTINATION_TABLE]
   --destination-region value, --dr value   aws region of destination table (default: "us-east-1") [$DESTINATION_REGION]
   --destination-account value, --da value  AWS Account number of the destination table [$DESTINATION_ACCOUNT]
   --role value, -r value                   AWS Role Name to assume for source/dest accounts. Must be present in both accounts [$ROLE]
   --log-level value                        Log level (panic, fatal, error, warn, info, or debug) (default: "error") [$LOG_LEVEL]
```

- compare: checks item count between two dynamo tables (does not check for item equality, only count)
```
 $ dynamodb-migrator compare --help
 NAME:
    dynamodb-migrator compare - Compares item counts from Source Table and Destination Table

 USAGE:
    dynamodb-migrator compare [command options] [arguments...]

 OPTIONS:
    --source-table value, --st value         Required: Dynamodb Table Name to source data from [$SOURCE_TABLE]
    --source-region value, --sr value        aws region of source table (default: "us-east-1") [$SOURCE_REGION]
    --source-account value, --sa value       AWS Account number of the source table [$SOURCE_ACCOUNT]
    --destination-table value, --dt value    Required: Dynamodb Table Name to write to [$DESTINATION_TABLE]
    --destination-region value, --dr value   aws region of destination table (default: "us-east-1") [$DESTINATION_REGION]
    --destination-account value, --da value  AWS Account number of the destination table [$DESTINATION_ACCOUNT]
    --role value, -r value                   AWS Role Name to assume for source/dest accounts. Must be present in both accounts [$ROLE]
    --log-level value                        Log level (panic, fatal, error, warn, info, or debug) (default: "error") [$LOG_LEVEL]
```
## Installing
Package management with dep.

Running:
```
dep ensure
go install
dynamodb-migrator
```

## A couple things to consider
- This does no transformation of the data or the tables. Indexes / Attributes must be the same across tables
- The program assumes whatever IAM credentials are provided to the environment it runs in. It will need `dynamodb:Scan` permissions on the source table, `dynamodb:BatchWriteItem, dynamodb:PutItem` permissions on the destination table, and `dynamodb:UpdateTable` permissions on each table
- Depending on table size and read / write capacity this process can run for a long time. Failures for `ProvisionedThroughputExceeded` are retried indefinitely. Raising the read/write capacity of the respective source/destination table can speed things up, but this process will likely consume all read/write capacity of the tables at some point.
- You can run this multiple times but items already in the destination table will not be updated as `BatchWriteItem` does not do updates
