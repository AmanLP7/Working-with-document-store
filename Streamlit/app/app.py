########################################## Python web app to retrieve invoice data #####################################

'''
The code for creating a python web-app which takes customer or invoice id as input
and retrieves invoice data. 
'''

########################################################################################################################

# Importing required modules

# System library imports
import requests                 # For makinh HTTP requests

# 3rd party modules
import numpy as np              # For numercial computation
import streamlit as st          # For creating streamlit web-app
import pandas as pd             # For working with dataframes


########################################################################################################################


# Add input field for customer id
customer_id = st.sidebar.text_input("CustomerID:")

# If customer ID has been entered
if customer_id:
    URL = "http://localhost:8000/customer/"
    data = requests.get(URL+customer_id).json()
    df = pd.DataFrame(data)
    df.drop_duplicates(subset="InvoiceNo", keep="first", inplace=True)

    # Add the table
    st.header("Output customer invoices")
    data = st.dataframe(df)

invoice_no = st.sidebar.text_input("InvoiceNo:")

# If invoice number has been entered
if invoice_no:
    URL = "http://localhost:8000/invoice/"
    data = requests.get(URL+invoice_no).json()
    df = pd.DataFrame(data)
    reindexed = df.reindex(sorted(df.columns), axis=1)
    st.header("Output by invoice ID")
    data = st.dataframe(df)