import csv
import io
import random
from datetime import datetime, timedelta
import boto3

# Initialize S3 client
s3 = boto3.client('s3')

# Configuration
BUCKET_NAME = 'lambda-stcoks-data'
STOCK_SYMBOLS = [
    "AAPL", "MSFT", "GOOG", "AMZN", "FB", "TSLA", "NFLX", "NVDA", "BABA", "INTC",
    "CSCO", "ADBE", "ORCL", "IBM", "PYPL", "CRM", "QCOM", "AMD", "TXN", "AVGO",
    "COST", "SHOP", "SPOT", "UBER", "LYFT", "SQ", "TWTR", "SNAP", "DOCU", "ZM",
    "ROKU", "EA", "ATVI", "MRNA", "PFE", "JNJ", "KO", "PEP", "DIS", "V", "MA",
    "GS", "JPM", "BAC", "WMT", "TGT", "HD", "CVX", "XOM", "NKE", "MCD"
]  # up to 50 symbols

# Function to generate dummy stock data
def generate_stock_data(days=30):
    data = []
    today = datetime.now()
    price = random.uniform(100, 500)  # initial price
    for i in range(days):
        date = today - timedelta(days=i)
        open_price = price * random.uniform(0.98, 1.02)
        close_price = open_price * random.uniform(0.98, 1.02)
        high_price = max(open_price, close_price) * random.uniform(1.0, 1.02)
        low_price = min(open_price, close_price) * random.uniform(0.98, 1.0)
        volume = random.randint(1000000, 10000000)
        data.append({
            "date": date.strftime('%Y-%m-%d'),
            "open": round(open_price, 2),
            "close": round(close_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "volume": volume
        })
        price = close_price  
    return list(reversed(data))  

def save_csv_to_s3(symbol, data):
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["date", "open", "close", "high", "low", "volume"])
    writer.writeheader()
    writer.writerows(data)

    # FIX: Define timestamp here
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=f"stocks/{symbol}{timestamp}.csv",
        Body=csv_buffer.getvalue()
    )
    print(f"Uploaded {symbol}-{timestamp}.csv to S3")

# Lambda handler
def lambda_handler(event, context):
    for symbol in STOCK_SYMBOLS:
        data = generate_stock_data(days=30)
        save_csv_to_s3(symbol, data)
    return {
        "statusCode": 200,
        "body": f"Successfully uploaded {len(STOCK_SYMBOLS)} stock CSVs to S3."
    }
