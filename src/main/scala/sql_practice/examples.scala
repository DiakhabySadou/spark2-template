package sql_practice

import spark_helpers.SparkSessionHelper
import org.apache.spark.sql.functions._
object examples {
    def exercise1(): Unit={

      val spark = SparkSessionHelper.getSparkSession()
      import spark.implicits._

      val tourDF = spark.read
          .option("multiline",true)
          .option("mode","PERMISSIVE")
          .json("data/input/tours.json")

      tourDF.show()
      println("Number of tours : "+tourDF.count())
println("Number of tours difficulties) ")
      tourDF.groupBy("tourDifficulty").count().show();

println("================statistics on the hole table====================")
      println(tourDF.agg(min("tourPrice"),max("tourPrice"),avg("tourPrice")).show())


println("================statistics on the hole table====================")
      val difTable = tourDF.groupBy("tourDifficulty")
      println(difTable.agg(min("tourPrice"),max("tourPrice"),avg("tourPrice")).show())

println("================statics on difficulty and duration ====================")
      println(difTable.agg(min("tourPrice"),max("tourPrice"),avg("tourPrice"),
        min("tourLength"), max("tourLength"), avg("tourLength")).show())

println("================Top 10 ====================")

      tourDF.select(explode($"tourTags")).groupBy("col").count().sort($"count".desc).limit(10).show()


println("================Relationship difficulty and top 10 ====================")
      tourDF.select(explode($"tourTags"),$"tourDifficulty").groupBy($"col",$"tourDifficulty").count().sort($"count".desc).limit(10).show()

println("================Min max of price in Relationship difficulty and top 10====================")
      val rel10 =tourDF.select(explode($"tourTags"),$"tourDifficulty",$"tourPrice").groupBy($"col",$"tourDifficulty",$"tourPrice").count().limit(10);

      rel10.agg(min("tourPrice"),max("tourPrice"),avg("tourPrice").as("avg")).sort($"avg".desc).show()

      tourDF.printSchema()
    }
}
