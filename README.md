# EmailLog_Rotator
## Lambda function to create montly DynamoDB tables for SES email logs.
* New tables are created using the format EmailLog-YYYYMM for the next calendar month.
* Downgrades ProvisionedThroughput to R1 W1 for last month's table.
* Deletes table for Email Log 1 yr old.

Triggered by CloudWatch Scheduled Event.
