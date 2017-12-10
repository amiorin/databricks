// Databricks notebook source
// MAGIC %md
// MAGIC ## Intro
// MAGIC Completeness check using window functions and offsets
// MAGIC 
// MAGIC https://blog.statsbot.co/sql-window-functions-tutorial-b5075b87d129

// COMMAND ----------

def toCustomerId(i: Long): String = (i % 10 + 65).toChar.toString

// COMMAND ----------

def toState(i: Long): String = {
  if ((i % 2) == 0) {
    "CA"
  } else {
    "NY"
  }
}

// COMMAND ----------

import java.sql.Timestamp
def toDatetime(i: Long) = new Timestamp(1512903644000L + (i * 3600 * 1000))

// COMMAND ----------

def toAmount(i: Long) = ((i % 20 + 1) * 50)

// COMMAND ----------

val df = spark.range(10000).map(c => (c, toCustomerId(c), toState(c), toDatetime(c), toAmount(c))).toDF("order_id", "customer_id", "state", "datetime", "amount")
df.createOrReplaceTempView("orders")

// COMMAND ----------

df.show

// COMMAND ----------

// MAGIC %sql
// MAGIC select cast(datetime as date) from orders

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT
// MAGIC trunc(datetime, "MM") as month,
// MAGIC sum(amount) as revenue
// MAGIC FROM orders
// MAGIC GROUP BY 1
// MAGIC ORDER BY 1

// COMMAND ----------

// MAGIC %sql
// MAGIC WITH
// MAGIC monthly_revenue as (
// MAGIC SELECT
// MAGIC trunc(datetime, "MM") as month,
// MAGIC sum(amount) as revenue
// MAGIC FROM orders
// MAGIC GROUP BY 1
// MAGIC )
// MAGIC ,prev_month_revenue as (
// MAGIC SELECT *,
// MAGIC lag(revenue) over (order by month) as prev_month_revenue
// MAGIC FROM monthly_revenue
// MAGIC )
// MAGIC SELECT *,
// MAGIC round(100.0*(revenue-prev_month_revenue)/prev_month_revenue,1) as revenue_growth
// MAGIC FROM prev_month_revenue
// MAGIC ORDER BY 1