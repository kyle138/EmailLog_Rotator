# EmailLog_Rotator
Lambda function to create montly DynamoDB tables for SES email logs.
New tables are created using the format EmailLog-YYYYMM for the next calendar month.
Deletes table for Email Log 1 yr old.
Triggered by CloudWatch Scheduled Event.
