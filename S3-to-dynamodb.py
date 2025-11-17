import boto3
import csv
import json
from decimal import Decimal

# Initialize AWS clients
s3 = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('stocks-data')  # Your DynamoDB table

def process_csv(bucket, key):
    """Reads a CSV file from S3 and returns a list of dictionaries."""
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8').splitlines()
    reader = csv.DictReader(content)
    return list(reader)

def safe_decimal(value):
    """Converts a value to Decimal; defaults to 0 if invalid."""
    try:
        return Decimal(str(value))
    except:
        return Decimal("0")

def extract_symbol_timestamp(key):
    """
    Extract symbol and timestamp from S3 key.
    Example key: stocks/LYFT/LYFT2025-11-17_15-26-55.csv
    """
    parts = key.split('/')
    if len(parts) < 2:
        raise ValueError(f"Invalid S3 key format: {key}")
    
    symbol = parts[1]
    filename = parts[-1].split('.')[0]  # LYFT2025-11-17_15-26-55
    # Remove symbol from start to get timestamp
    timestamp = filename.replace(symbol, "", 1)
    return symbol, timestamp  # timestamp format: 2025-11-17_15-26-55

def lambda_handler(event, context):
    if 'Records' not in event:
        return {"statusCode": 400, "message": "No SQS Records"}

    total_files = 0
    total_rows = 0

    for record in event['Records']:
        # SQS message body contains the S3 event as JSON string
        try:
            s3_event = json.loads(record['body'])
            s3_record = s3_event['Records'][0]['s3']
        except Exception as e:
            print(f"Error parsing SQS record: {e}")
            continue

        bucket = s3_record['bucket']['name']
        key = s3_record['object']['key']

        try:
            symbol, timestamp = extract_symbol_timestamp(key)
        except Exception as e:
            print(f"Error extracting symbol/timestamp from key {key}: {e}")
            continue

        # Read CSV
        try:
            rows = process_csv(bucket, key)
        except Exception as e:
            print(f"Error reading CSV {key}: {e}")
            continue

        # Insert into DynamoDB
        try:
            with table.batch_writer() as batch:
                for idx, row in enumerate(rows):
                    partition_key = f"{symbol}_{timestamp}"   # partition key
                    sort_key = f"{row['date']}_{idx}"        # unique sort key per row
                    batch.put_item(Item={
                        "symbol_timestamp": partition_key,  # partition key
                        "row_key": sort_key,               # sort key
                        "symbol": symbol,
                        "timestamp": timestamp,
                        "date": row["date"],
                        "open": safe_decimal(row["open"]),
                        "close": safe_decimal(row["close"]),
                        "high": safe_decimal(row["high"]),
                        "low": safe_decimal(row["low"]),
                        "volume": int(row["volume"])
                    })
            print(f"Processed {key}, inserted {len(rows)} rows")
            total_files += 1
            total_rows += len(rows)
        except Exception as e:
            print(f"Error inserting rows from {key}: {e}")

    return {
        "statusCode": 200,
        "message": f"Processed {total_files} files with {total_rows} rows into DynamoDB"
    }
