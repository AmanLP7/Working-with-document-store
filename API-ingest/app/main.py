################################# API to handle data from stream and interact with app #################################

'''
The API performs following functions -

1. Ingest data into a Kafka-stream
2. Handle get request from streamlit app
'''

########################################################################################################################


### Importing the required modules

# System library imports 
import json                                     # Module to create JSON dumps
from datetime import datetime                   # Working with date and time

# 3rd party modules
from fastapi import FastAPI                     # Create FastAPI requests
from fastapi import status, HTTPException       # Work with status and handle HTTP exceptions
from fastapi.encoders import jsonable_encoder   # Create JSON payload
from fastapi.responses import JSONResponse      # Returns a JSON response
from pydantic import BaseModel                  # Create JSON data models
from kafka import KafkaProducer, producer       # Work with Kafka streams
import pymongo                                  # Work with MongoDB


########################################################################################################################


def produce_kafka_string(json_string: object) -> None:
    ''' 
    Function to push message to Kafka stream
    ...

    Parameters
    ----------
    json_string (object):
        A payload as JSON string

    Returns
    -------
    None
    '''
    producer = KafkaProducer(bootstrap_servers="kafka:9092", acks=1)
    producer.send("ingestion-topic", bytes(json_string, "utf-8"))
    producer.flush()


class DB:
    ''' 
    Class containing methods and attributes to deal with
    MongoDB document store.
    ...

    Attributes
    ----------
    address (string):
        URL to reach mongoDB server
    username (string):
        Username for the server
    password (string):
        Password of the server
    client (object):
        a mongodb client
    database (str):
        name of the database
    table (str):
        name of the table
    '''

    address = "mongodb://mongo:27017/"
    username = "root"
    password = "example"

    def __init__(self, database: str, table: str) -> None:
        ''' 
        Function to instantiate the instance of class
        ...

        Parameters
        ----------
        database (str):
            Name of the database
        table (str):
            Name of the table

        Returns
        -------
        None
        '''

        self.client = pymongo.MongoClient(
            self.address,
            username=self.username,
            password=self.password
        )
        self.db = self.client[database]
        self.table = self.db[table]


    def find_customer(self, id: str) -> object:
        ''' 
        Function to information about invoices
        using customer id.
        ...

        Parameters
        ----------
        id (str):
            ID of the customer

        Returns
        -------
        A list of dictionaries
        '''

        data = []
        query = {"CustomerID": id}
        document = self.table.find(query)

        for d in document:
            data.append(d)

        return data

    
    def find_invoice(self, id: str) -> object:
        '''
        Function to find information about invoice
        using the invoice id.
        ...

        Parameters
        ----------
        id (str):
            ID of the invoice

        Returns
        -------
        A list of dictionaries
        '''

        data = []
        query = {"InvoiceNo": id}
        document = self.table.find(query)

        for d in document:
            data.append(d)

        return data


class InvoiceItem(BaseModel):
    ''' 
    Class containing attributes representing
    structure of a post API request
    ...

    Attributes
    ----------
    InvoiceNo (int):
        Invoice number of the order
    StockCode (str):
        Code of the product
    Description (str):
        Description of the product
    Quantity (int):
        Number of items of a product
    InvoiceDate (str):
        Date when the order was recorded
    UnitPrice (float):
        Cost of the product
    CustomerID (str):
        ID of the customer
    Country (str):
        Name of the country where order
        was recorded
    '''

    InvoiceNo: int
    StockCode: str
    Description: str
    Quantity: int
    InvoiceDate: str
    UnitPrice: float
    CustomerID: str
    Country: str


# FastAPI decorator for general execution
app = FastAPI()

# Initialsing DB object
mongodb = DB(database="docstreaming", table="invoices")


@app.get("/")
async def root() -> object:
    '''
    Function to display message for
    the root address.
    ...

    Parameters
    ----------
    None

    Returns
    -------
    None
    '''

    return {"message": "Welcome to retail!"}


@app.post("/invoiceitem")
async def post_invoice_item(item: InvoiceItem) -> object:
    ''' 
    Function to add new invoice data.
    ...

    Parameters
    ----------
    item (InvoiceItem):
        JSON object with invoice details

    Returns
    -------
    JSON object
    '''
    
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


@app.get("/customer/{customer_id}")
async def read_customer_data(customer_id: str) -> object:
    '''
    Function to get customer id data 
    given customer ID.
    ...

    Parameters
    ----------
    customer_id (str):
        ID of the customer

    Returns
    -------
    A JSON object with customer information
    '''

    try:

        data = []
        item = mongodb.find_customer(customer_id)

        for i in item:
            data.append(
                InvoiceItem(
                    InvoiceNo= i["InvoiceNo"],
                    StockCode= i["StockCode"],
                    Description= i["Description"],
                    Quantity= i["Quantity"],
                    InvoiceDate= i["InvoiceDate"],
                    UnitPrice= i["UnitPrice"],
                    CustomerID= i["CustomerID"],
                    Country= i["Country"]
                )
            )
    
        # Converting the data into JSON
        data = jsonable_encoder(data)

        return JSONResponse(content=data)

    except:

        raise HTTPException(status_code=404, detail="Item not found")


@app.get("/invoice/{invoice_id}")
async def read_invoice_data(invoice_id: str) -> object:
    '''
    Function to get invoice data given
    invoice ID.
    ...

    Parameters
    ----------
    invoice_id (str):
        ID of the invoice

    Returns
    -------
    A JSON object with invoice information
    '''

    try:

        data = []

        # Get data from mongodb
        item = mongodb.find_invoice(invoice_id)
        
        for i in item:
            data.append(
                InvoiceItem(
                    InvoiceNo= i["InvoiceNo"],
                    StockCode= i["StockCode"],
                    Description= i["Description"],
                    Quantity= i["Quantity"],
                    InvoiceDate= i["InvoiceDate"],
                    UnitPrice= i["UnitPrice"],
                    CustomerID= i["CustomerID"],
                    Country= i["Country"]
                )
            )

        # Converting the data into JSON
        data = jsonable_encoder(data)

        return JSONResponse(content=data)

    except:

        raise HTTPException(status_code=404, detail="Item not found")

