# Databricks notebook source


# COMMAND ----------

var = df['dsfa']:
        "format": df["dfsd"]
        nos

# COMMAND ----------

y = yaml(var)

# COMMAND ----------

"ordersandshipments":
        "format": "csv"
        "number_of_columns": 22
        "datatypes": {
            "SO Number": "str",
            "PO Number": "str",
            "Customer": "str",
            "Order Date": "date",
            "Ship To State": "str",
            "Ship To Zip": "str",
            "Ship To Address": "str",
            "RDC Code": "str",
            "Store Code": int,
            "Carrier Code": "str",
            "DATE FULFILLED": "date",
            "Earliest Delivery Date": "date",
            "Latest Delivery Date": "date",
            "Item Number": "str",
            "Item Ordered": "str",
            "Item Price": "float",
            "Quantity": "float",
            "Quantity Fulfilled/Received": "float",
            "Qty Invoiced": "float",
            "EACHES IN A CASE": "float",
            "UPC Code": "int",
            "UPC Case Code": "int",
        }
        "foreign_keys" : ["Item Number", "SO Number", "DATE FULFILLED"]
