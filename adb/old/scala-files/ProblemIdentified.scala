// Databricks notebook source
val s = 
"""
    >=1,0,0,Y,Postpaid
    0,1,0,N,NA
    0,0,1,N,NA
    >=1,1,1,N,NA
    >=1,1,0,N,NA
    >=1,0,1,N,NA
    0,>=1,>=1,N,NA
    0,1,1,Y,Fixed
"""

l = s
.trim()
.split("\n")
.map(_.trim)
.mkString("\n")
.split("\n")
.map(x=>x.split(","))
.map(x=>(x(0),x(1),x(2),x(3),x(4)))
.toList


// COMMAND ----------

Seq(l:_*).toDF("a", "b", "c", "d", "e").show
