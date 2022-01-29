# Modules required to use FastAPI, work
# with status and handle HTTP exceptions
from fastapi import FastAPI, status, HTTPException

# Modules required to turn classes into
# JSON and return JSON payload
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

# Module for creating JSON dumps
import json

# Create base model
from pydantic import BaseModel

# System library imports
from datetime import datetime

# Work with Kafka streams
from kafka import KafkaProducer, producer

# Create class (schema) for JSON
# date gets ingested as string and
# then converted to datetime
class InvoiceItem(BaseModel):
    InvoiceNo: int
    StockCode: str
    Description: str
    Quantity: int
    InvoiceDate: str
    UnitPrice: float
    CustomerID: str
    Country: str

# For general execution
app = FastAPI()

# Base URL
@app.get("/")
async def root():
    return {"message": "Hello world!"}

# Add a new invoice
@app.post("/invoiceitem")
async def post_invoice_item(item: InvoiceItem):
    print("Message received!")
    try:
        # Evaluate the datetime string and convert
        # it into a python datetime object
        date = datetime.strptime(item.InvoiceDate, "%d/%m/%Y %H:%M")
        print(f"Found a timestamp: {date}")

        item.InvoiceDate = date.strftime("%d-%m-%Y %H:%M:%S")
        print(f"New item date: {item.InvoiceDate}")

        # Parse the item to JSON
        item_json = jsonable_encoder(item)

        # Dump the JSON as string
        item_string = json.dumps(item_json)
        print(item_string)

        # Produce Kafka string
        produce_kafka_string(item_string)

        return JSONResponse(content=item_json, status_code=201)

    except ValueError:
        return JSONResponse(content=jsonable_encoder(item), status_code=400)


def produce_kafka_string(json_string):
    producer = KafkaProducer(bootstrap_servers="kafka:9092", acks=1)
    producer.send("ingestion-topic", bytes(json_string, "utf-8"))
    producer.flush()
