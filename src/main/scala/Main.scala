package com.mycompany.project.model
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, regexp_replace, when}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(".data/4.1. test_file.csv")

    df.printSchema()

    df.show(20, truncate = false)

    df.show(200, truncate = false)

    df.filter(col("Payers_name").isNull)
      .show(50, truncate = false)
    println(df.filter(col("Payers_name").isNull).count())

    val filderedDF = df.filter(col("Payers_name").isNull)
    val nullCount = filderedDF.count()
    println(s"$nullCount")

    df.filter(col("Payers_name").isNotNull)
      .show(200, truncate = false)
    println(df.filter(col("Payers_name").isNotNull).count())

    val filteredDFNot = df.filter(col("Payers_name").isNotNull)
    val nullCountNot = filteredDFNot.count()
    println(s"$nullCountNot")

    df.groupBy(col("Date_of_debiting_or_crediting"))
      .count()
      .show(200, truncate = false)

    df.withColumn("Balancing_balance",
      regexp_replace(regexp_replace(col("Balancing_balance"), " ", ""), ",", ".").cast("double"))
      .agg(max("Balancing_balance"), avg("Balancing_balance"), min("Balancing_balance"))
      .show()

    df.withColumn("Payment_amount",
      regexp_replace(regexp_replace(col("Payment_amount"), " ", ""), ",", ".").cast("double"))
      .withColumn("isPayment_amount", when(col("Payment_amount") < 0, "bad").when(col("Payment_amount") > 0, "good"))
      .show(200, truncate = false)
  }
}
