# Databricks notebook source
def display_version(version):
    print(version)

# COMMAND ----------

's' in ['a', 'b', 's']

# COMMAND ----------

a + b

# COMMAND ----------

display_version(3.4)

# COMMAND ----------

secret_owner = "sudhir"

# COMMAND ----------

class Phone:
    def __init__(self, brand, os, form_factor, price, imei):
        self.brand = brand
        self.os = os
        self.form_factor = form_factor
        self.price = price
        self.imei = imei

    def __iter__(self):
        return self
    
    def __next__(self):
        if self.price >= float(70000):
            raise StopIteration
        self.price = self.price + float(1000)
        return self.price

    def __str__(self):
        p = f"{self.brand, self.form_factor, self.price, self.price}"
        return p
    
    def __repr__(self):
        p = f"{self.brand, self.form_factor, self.price, self.price}"
        return p
    
    def __eq__(self, other):
        if self.brand == other.brand:
            return True
        else:
            return False

    def __add__(self, other):
        return (float(self.price) + float(other.price))
    
    def __sub__(self, other):

        return (float(self.price) - float(other.price))

    def set_owner(self, name, phone, pan):
        self.owner_info = {
            "name": name
            , "phone": phone
            , "pan": pan
        }
    
    def display_version(self, version):
        print(version, "of phone")


# COMMAND ----------

a = Phone("samsung", "android", "6", 59000, "XXX")
b = Phone("samsung", "android", "11", 29000, "XXXXXX")

# COMMAND ----------

for each in a:
    print(each)

# COMMAND ----------

a + b

# COMMAND ----------

a - b

# COMMAND ----------

a

# COMMAND ----------

print(a)

# COMMAND ----------

a.display_version()

# COMMAND ----------

b = Phone("mi", "android", "11", "29000", "XXXXXX")

# COMMAND ----------

a.set_owner("sudhir", "8587001379", "xxxxx")

# COMMAND ----------

a.owner_info

# COMMAND ----------

# A class unusal functionality

# COMMAND ----------

class NoBracket(type):
    def __getattr__(cls, name):
        method = getattr(cls(), name)
        if callable(method):
            return method()
        return method
    
class sudhir(metaclass=NoBracket):
    def show(self):
        print("sudhir")


# COMMAND ----------

sudhir.show

# COMMAND ----------


