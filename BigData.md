# Big Data Technologies
## MapReduce
### Wordcount
```pig
lines = LOAD '/path/to/file.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words by word;
wordcount = FOREACH grouped GENERATE group, COUNT(words);
DUMP wordcount;
```

```hql
CREATE TABLE docs (line STRING);
LOAD DATA INPATH 'docs' OVERWRITE INTO TABLE docs;
CREATE TABLE word_counts AS
SELECT word, count(1) AS count FROM
(SELECT explode(split(line, '\\s')) AS word FROM docs) w
GROUP BY word
ORDER BY word;
```

```scala
val text_rdd = spark.read.cvs("hdfs://path").toRDD
val counts = text_rdd.flatMap(lambda line: line.split(" ")) \
                     .map(lambda word: (word, 1)) \
                     .reduceByKey(_ + _)
coounts.write.csv("hdfs://path")
```

```scala
val linesDF = spark.read.csv("").toDF("line")
val wordsDF = linesDF.explode("line", "word")((line: string) => line.split(" "))
val wordCount = wordsDF.groupBy("word").count()
```

### Top k in each group
```pig
records = LOAD '/path/to/file.txt' AS (id:chararray, group_id:chararray, val:chararray);
grouped = GROUP records BY group_id;
topK = FOREACH grouped {
  sorted = ORDER records BY id DESC;
  top = TOP(K, 0, sorted);
  GENERATE top;
}
DUMP topK;
```

```hive
SELCT *
FROM (
  SELECT  *
          , rand() over (partition by id, order by value desc) as rank
  FROM table t
  )
WHERE rank < K
```

```scala
df.select(col).orderBy(order_col.desc).limit(K).show(false)
```

```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.*

val partitionedWindow = Window.partitionBy(id_col).orderBy(date_cole.desc)
val rankTest = rank().over(partitionedWindow)
df.select(*, rankTest as "rank").show()
df.filter(rank().over(partitionedWindow) <= K).show()

df.withColumn("rank", rank().over(Window.partitionBy("id").orderBy($"value".desc)))
  .filter($"rank" <= K)
  .drop("rank")
```
