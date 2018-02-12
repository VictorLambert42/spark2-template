package sql_practice

import spark_helpers.SparkSessionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.explode

object examples {
  def exec1(): Unit ={
    val spark = SparkSessionHelper.getSparkSession()
    import spark.implicits._

    val tourDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    println(tourDF.count)

    tourDF.printSchema

    println(tourDF.groupBy("tourDifficulty").count.count)

    tourDF
      .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"))
      .show

    tourDF.groupBy("tourDifficulty")
      .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"))
      .show

    tourDF.groupBy("tourDifficulty")
      .agg(min("tourPrice"), max("tourPrice"), avg("tourPrice"),min("tourLength"), max("tourLength"), avg("tourLength"))
      .show

    tourDF.select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .sort($"count".desc)
      .show(10)

    tourDF.select(explode($"tourTags"), $"tourDifficulty", $"tourPrice")
      .groupBy($"col", $"tourDifficulty")
      .agg(min($"tourPrice"), max($"tourPrice"), avg($"tourPrice"))
      .sort($"avg(tourPrice)".desc)
      .show(10)
  }
}
