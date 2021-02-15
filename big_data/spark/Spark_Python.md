# PySpark

## DataFrame
```python
df = (spark.read
           .option("header", "true")
           .option("sep", "|")
           .option("inferSchema", "true")
           .csv("/path/to/file.csv")
     )
df.show(2, False)

df.filter("Country" = "USA")

df.groupBy("Country").count().show()

df2 = (df.withColumn("Country_Code", split("Phone", "-").getItem(0))
         .withColumn("Phone_No", split("Phone", "-").getItem(1))
      )
df2.show()
```
