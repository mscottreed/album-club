# Databricks notebook source
# MAGIC %md
# MAGIC ### Exploring Input Data
# MAGIC
# MAGIC Use this notebook to explore the input data.
# MAGIC
# MAGIC **Note**: This notebook is not executed as part of the pipeline.

# COMMAND ----------

import sys

sys.path.append("/Workspace/Users/scott@craftingbytes.com/Album Review Pipeline")

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1001 Albums CSV

# COMMAND ----------

df = spark.read.format("csv").option("header", True).load("/Volumes/workspace/music/albums-1001-before-you-die/albums.csv")
display(df.limit(5))

# COMMAND ----------

display(df.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Best selling albums CSV

# COMMAND ----------

df2 = spark.read.format("csv").option("header", True).load("/Volumes/workspace/music/albums-best-selling/Top_100_best_selling_albums_worldwide.csv")
display(df2.limit(5))

# COMMAND ----------

display(df2.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Album Ratings CSV

# COMMAND ----------

df3 = spark.read.format("csv").option("header", True).load("/Volumes/workspace/music/album-club-ratings/Album_Ratings_Complete.csv")

# COMMAND ----------

display(df3.limit(20))

# COMMAND ----------

display(df3.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Album tracks CSVs

# COMMAND ----------

df4 = spark.read.format("csv").option("header", True).load("/Volumes/workspace/music/albums-1001-tracks/0001-Frank Sinatra-In the Wee Small Hours.csv")

# COMMAND ----------

display(df4.columns)
