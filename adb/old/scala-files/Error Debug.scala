// Databricks notebook source
        def whenCondMethod(whenCond: String): org.apache.spark.sql.Column = {
            var condValue: org.apache.spark.sql.Column = null
            var cond = whenCond.split(Constants.PIPE).toSeq
            val y = cond.head.split(Constants.COLON)
            if (y(1).toString.equalsIgnoreCase(y(2).toString)) condValue = when(expr(y(0)), lit(expr(y(1).toString))) else condValue = when(expr(y(0)), lit(y(1).toString))
            cond.tail.foreach { x =>
                val z = x.split(Constants.COLON)
                if (z(1).toString.equalsIgnoreCase(z(2).toString)) condValue = condValue.when(expr(z(0)), lit(expr(z(1).toString))) else condValue = condValue.when(expr(z(0)), lit(z(1).toString))
            }
            condValue = condValue.otherwise(Constants.NOCHANGESTATUS)
            condValue
        }

// COMMAND ----------


