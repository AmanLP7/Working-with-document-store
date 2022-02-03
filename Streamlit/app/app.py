import numpy as np
import streamlit as st
import pandas as pd
import pymongo

client = pymongo.MongoClient(
    "mongodb://mongo:27017/",
    username="root",
    password="example"
    )

db = client["docstreaming"]
table = db["invoices"]

# Add input field for customer id
customer_id = st.sidebar.text_input("CustomerID:")

# If customer ID has been entered
if customer_id:
    query = {"CustomerID": customer_id}
    document = table.find(
        query,
        { "_id": 0, "StockCode": 0, "Description": 0, "Quantity": 0, "Country": 0, "UnitPrice": 0}
        )
    df = pd.DataFrame(document)
    df.drop_duplicates(subset="InvoiceNo", keep="first", inplace=True)

    # Add the table
    st.header("Output customer invoices")
    data = st.dataframe(df)

invoice_no = st.sidebar.text_input("InvoiceNo:")

# If invoice number has been entered
if invoice_no:
    query = {"InvoiceNo": invoice_no}
    document = table.find(
        query,
        { "_id": 0, "InvoiceDate": 0, "Country": 0, "CustomerID": 0 }
        )
    df = pd.DataFrame(document)
    reindexed = df.reindex(sorted(df.columns), axis=1)
    st.header("Output by invoice ID")
    data = st.dataframe(df)