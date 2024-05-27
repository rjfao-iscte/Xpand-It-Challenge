// Databricks notebook source
// MAGIC %md
// MAGIC Load Maven Dependencies

// COMMAND ----------

<dependencies>
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.5.1</version>
    <scope>provided</scope>
  </dependency>
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.12</artifactId>
    <version>3.5.1</version>
    <scope>provided</scope>
  </dependency>
</dependencies>


// COMMAND ----------

// MAGIC %md
// MAGIC Import Libraries

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


// COMMAND ----------

// MAGIC %md
// MAGIC Part 1

// COMMAND ----------

object review1 {

  
    //Create Spark session
    val spark_reviews = SparkSession.builder()
      .appName("Google Play Store User Review Analysis")
      .master("local[*]")
      .getOrCreate()

    // Read the CSV file into a DataFrame
    val filePath = "/FileStore/tables/googleplaystore_user_reviews.csv"
    val df: DataFrame = spark_reviews.read
      .option("header", "true")
      .option("inferSchema", "true").csv(filePath)
   

    //PART 1

    // Filter out non-numeric and null values from Sentiment_Polarity
    val dfFiltered_part1 = df.filter(col("Sentiment_Polarity").isNotNull && col("Sentiment_Polarity").rlike("^-?\\d*\\.?\\d+$"))
    dfFiltered_part1.show()

    // Change column datatype
    val dfWithDoublePolarity_part1 = dfFiltered_part1.withColumn("Sentiment_Polarity", col("Sentiment_Polarity").cast(DoubleType))

    // Calculate average sentiment polarity grouped by App
    val df_1 = dfWithDoublePolarity_part1
      .groupBy("App")
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity"))

  
  
}

// COMMAND ----------

val df = review1.df_1
//df.show()
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Part 2

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.functions._

object google2 {
val spark_google = SparkSession.builder()
      .appName("Google Play Store Analysis")
      .master("local[*]")
      .getOrCreate()

    // Read the CSV file into a DataFrame
    val filePath = "/FileStore/tables/googleplaystore.csv"
    val df:DataFrame = spark_google.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

    // Filter out non-numeric and null values from Rating
    val dfFiltered_part2 = df
      .filter(col("Rating").isNotNull && col("Rating").rlike("^-?\\d*\\.?\\d+$"))
    dfFiltered_part2.show()


    // Change column datatype to DoubleType
    val dfWithDouble_part2 = dfFiltered_part2
    .withColumn("Rating", col("Rating").cast(DoubleType))
    dfWithDouble_part2.printSchema()

    // Filter rows with Rating >= 4.0 and sort by Rating in descending order
    val df_2 = dfWithDouble_part2
      .filter(col("Rating") >= 4.0).orderBy(col("Rating").desc)
  
    // Save the DataFrame as a CSV file with the specified delimiter
    val outputFilePath = "/FileStore/tables/best_apps/csv"
    df_2.write
    .option("delimiter", "§")
    .option("header", "true")
    .csv(outputFilePath)
}

// COMMAND ----------

//google2.filteredDf_part2.show()
display(google2.df_2)

// COMMAND ----------

// MAGIC %md
// MAGIC Part 3

// COMMAND ----------

import scala.collection.Seq
object google3{

val spark_google = SparkSession.builder()
      .appName("Google Play Store Analysis")
      .master("local[*]")
      .getOrCreate()

// Read the CSV file into a DataFrame
val filePath = "/FileStore/tables/googleplaystore.csv"
val df:DataFrame = spark_google.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(filePath)

// Convert the "Last Updated" column to date type
val dfConverted = df.withColumn("Last Updated",
  date_format(to_date($"Last Updated", "MMMM d, yyyy"), "yyyy-MM-dd HH:mm:ss"))

// Remove non-numeric characters from the Size column
val dfCleaned = dfConverted.withColumn("Size", regexp_replace($"Size", "[^0-9]", ""))

val dfWithDataType = dfCleaned
.withColumn("Reviews", col("Reviews").cast(LongType))
.withColumn("Rating", col("Rating").cast(DoubleType))
.withColumn("Size", col("Size").cast(DoubleType))
.withColumn("Price", col("Price").cast(DoubleType))
.withColumn("Last Updated", col("Last Updated").cast(DateType))

val aggregatedDF = dfWithDataType.groupBy("App").agg(
  collect_set("Category").as("Categories"),
  first("Rating", ignoreNulls = true).as("Rating"),
  max("Reviews").as("MaxReviews"),
  first("Size", ignoreNulls = true).as("Size"),
  first("Installs", ignoreNulls = true).as("Installs"),
  first("Type", ignoreNulls = true).as("Type"),
  first("Price", ignoreNulls = true).as("Price"),
  first("Content Rating", ignoreNulls = true).as("ContentRating"),
  first("Genres", ignoreNulls = true).as("Genres"),
  first("Last Updated", ignoreNulls = true).as("LastUpdated"),
  first("Current Ver", ignoreNulls = true).as("CurrentVer"),
  first("Android Ver", ignoreNulls = true).as("AndroidVer")
)

// Join to get the row with the maximum number of reviews
val finalDF = dfWithDataType.as("original")
  .join(aggregatedDF.as("aggregated"), Seq("App"))
  .filter(col("original.Reviews") === col("aggregated.MaxReviews"))
  .select(
    col("original.App"),
    col("aggregated.Categories"),
    col("original.Rating"),
    col("original.Reviews"),
    col("original.Size"),
    col("original.Installs"),
    col("original.Type"),
    col("original.Price"),
    col("original.Content Rating").as("ContentRating"),
    col("original.Genres"),
    col("original.Last Updated").as("LastUpdated"),
    col("original.Current Ver").as("CurrentVer"),
    col("original.Android Ver").as("AndroidVer")
  )

  val df_3 = finalDF
    .withColumn("PriceEuro", (col("Price") * 0.9))
    .withColumnRenamed("Categories", "Category")
    .withColumnRenamed("ContentRating", "Content Rating")
    .withColumnRenamed("CurrentVer", "Current Ver")
    .withColumnRenamed("AndroidVer", "Android Ver")
    .withColumnRenamed("LastUpdated", "Last Updated")
    .drop("Price")
    .withColumnRenamed("PriceEuro", "Price")
}


// COMMAND ----------

val df = google3.df_3
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC Part 4

// COMMAND ----------

// Register DataFrames as temporary views
review1.df_1.createOrReplaceTempView("exercise1")
google3.df_3.createOrReplaceTempView("exercise3")

// Merge the DataFrames
val mergedDF = spark.sql("""
  SELECT
    e3.*,
    e1.Average_Sentiment_Polarity
  FROM
    exercise3 e3
  LEFT JOIN
    exercise1 e1
  ON
    e3.App = e1.App
""")

// Save the final DataFrame as Parquet with gzip compression
val outputPath = "/FileStore2/tables/googleplaystore_cleaned"
mergedDF
  .write
  .option("compression", "gzip")
  .parquet(outputPath)

// COMMAND ----------

display(mergedDF)

// COMMAND ----------

// MAGIC %md
// MAGIC Part 5

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, functions}

val df_teste = mergedDF.select($"App", $"Genres", $"Rating", $"Average_Sentiment_Polarity")
// Calculate metrics by Genre
val df_4 = df_teste
  .groupBy($"Genres")
  .agg(
    countDistinct($"App").alias("Count"),
    avg($"Rating").alias("Average_Rating"),
    avg($"Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
  )

// Save the final DataFrame as Parquet with gzip compression
val outputPath = "/FileStore1/tables/googleplaystore_metrics"

df_4
  .write
  .option("compression", "gzip")
  .parquet(outputPath)


// COMMAND ----------

display(df_4)
