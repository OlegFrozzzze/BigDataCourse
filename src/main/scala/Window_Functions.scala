package com.mycompany.project.model
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, count, cume_dist, dense_rank, first_value, lag, last_value, lead, max, min, ntile, percent_rank, rank, row_number, sum}

object Window_Functions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark")
      .master("local[*]")
      .getOrCreate()

    val df = spark.read
      .option("header", "true")
      .option("delimiter", ";")
      .csv(".data/2.1. test_file.csv")

    val windowSpecRowNumber = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultRN = df.withColumn("row_number", row_number().over(windowSpecRowNumber))
    resultRN.show(200, truncate = false)

    val windowSpecRank = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultR = df.withColumn("rank", rank().over(windowSpecRank))
    resultR.show(200, truncate = false)

    val windowSpecDenseRank = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultDR = df.withColumn("dense_rank", dense_rank().over(windowSpecDenseRank))
    resultDR.show(200, truncate = false)

    val windowSpecPercent_Rank = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultPR = df.withColumn("percent_rank", percent_rank().over(windowSpecPercent_Rank))
    resultPR.show(200, truncate = false)

    val windowSpecNtile = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultNtile = df.withColumn("quartile", ntile(4).over(windowSpecNtile))
    resultNtile.show(200, truncate = false)

    val windowSpecLead = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultL = df.withColumn("Next_Salary", lead("Salary", 1).over(windowSpecLead))
    resultL.show(200, truncate = false)

    val windowSpecLag = Window.partitionBy("Department").orderBy(col("Salary").asc)
    val resultLag = df.withColumn("Prev_Salary", lag("Salary", 1).over(windowSpecLag))
    resultLag.show(200, truncate = false)

    val windowSpecFV = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultFV = df.withColumn("First_Salary", first_value(col("Salary")).over(windowSpecFV))
    resultFV.show(200, truncate = false)

    val windowSpecLV = Window.partitionBy("Department").orderBy(col("Salary").desc)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val resultLV = df.withColumn("Last_Salary", last_value(col("Salary")).over(windowSpecLV))
    resultLV.show(200, truncate = false)

    val windowSpecAVG = Window.partitionBy("Department").orderBy(col("Salary").desc)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val resultAVG = df.withColumn("AVG_Salary", avg("Salary").over(windowSpecAVG))
    resultAVG.show(200, truncate = false)

    val windowSpecSum = Window.partitionBy("Department").orderBy(col("Salary").desc)
      .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    val resultSum = df.withColumn("Sum_Salary", sum("Salary").over(windowSpecSum))
    resultSum.show (200, truncate = false)

    val windowSpecMin = Window.partitionBy("Department").orderBy(col("Salary").asc)
    val resultMin = df.withColumn("Min_Salary", min("Salary").over(windowSpecMin))
    resultMin.show (200, truncate = false)

    val windowSpecMax = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultMax = df.withColumn("Max_Salary", max("Salary").over(windowSpecMax))
    resultMax.show (200, truncate = false)

    val windowSpecCount = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultCount = df.withColumn("Count_employees", count(col("Name")).over(windowSpecCount))
    resultCount.show (200, truncate = false)

    val windowSpecCume_Dist = Window.partitionBy("Department").orderBy(col("Salary").desc)
    val resultCume_Dist = df.withColumn("Cume_Dist", cume_dist().over(windowSpecCume_Dist))
    resultCume_Dist.show (200, truncate = false)
  }
}
