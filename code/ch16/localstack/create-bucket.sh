#!/bin/bash
# Run by LocalStack when it is ready: creates the invoices bucket
awslocal s3 mb s3://invoices
