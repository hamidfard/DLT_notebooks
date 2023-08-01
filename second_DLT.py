# Databricks notebook source
import dlt
from pyspark.sql.functions import expr

rules = {}
rules["valid_website"] = "(Website IS NOT NULL)"
rules["valid_location"] = "(Location IS NOT NULL)"
quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))

@dlt.table(
  name="raw_farmers_market"
)
def get_farmers_market_data():
  return (
    spark.read.format('csv').option("header", "true")
      .load('/databricks-datasets/data.gov/farmers_markets_geographic_data/data-001/')
  )

@dlt.table(
  name="farmers_market_quarantine",
  temporary=True,
  partition_cols=["is_quarantined"]
)
@dlt.expect_all(rules)
def farmers_market_quarantine():
  return (
    dlt.read("raw_farmers_market")
      .select("MarketName", "Website", "Location", "State",
              "Facebook", "Twitter", "Youtube", "Organic", "updateTime")
      .withColumn("is_quarantined", expr(quarantine_rules))
  )

@dlt.view(
  name="valid_farmers_market"
)
def get_valid_farmers_market():
  return (
    dlt.read("farmers_market_quarantine")
      .filter("is_quarantined=false")
  )

@dlt.view(
  name="invalid_farmers_market"
)
def get_invalid_farmers_market():
  return (
    dlt.read("farmers_market_quarantine")
      .filter("is_quarantined=true")
  )
