package com.talentorigin.spark.on.local

import com.talentorigin.spark.utilities.SparkSessionInitializer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, when}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingExample {
  def main(args: Array[String]): Unit = {

    val spark = SparkSessionInitializer.getSparkSession("local", "Spark Streaming Example")
    val sparkStreaming = new StreamingContext(spark.sparkContext, Seconds(2))
    val csvSchema = StructType(
      StructField("scheme_code", IntegerType, true)::
        StructField("scheme_name", StringType, true)::
        StructField("net_asset_value", DoubleType, true)::
        StructField("repurchase_price", DoubleType, true)::
        StructField("sale_price", DoubleType, true)::
        StructField("date", StringType, true):: Nil
    )

    val data = spark.readStream.schema(csvSchema).option("delimiter", ";").option("header","true").csv(args(0))
//    data.show()

    val correctedData = data.withColumn("repurchase_price", when(data("repurchase_price").isNull, 0.0).otherwise(data("repurchase_price")))
      .withColumn("sale_price", when(data("sale_price").isNull, 0.0).otherwise(data("sale_price")))

    val diffPriceData = correctedData.withColumn("diff_price", dailyDifference()(correctedData("repurchase_price"), correctedData("sale_price")))

    diffPriceData.writeStream
      .format("console")
      .queryName("diffCounts")
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()


/*    val sparkConf = SparkSessionInitializer.getSparkConf("local", "Spark Streaming Example")
    val sparkStream = new StreamingContext(sparkConf, Seconds(2))

    val streamData = sparkStream.textFileStream(args(0))
    val dataset = streamData.map(convertToMFRawDataObject)
    dataset.foreachRDD {
      rdd =>
        val correctedData = rdd.map{
          obj =>
            val rp = if(obj.repurchase_price == null) 0.0 else obj.repurchase_price
            val sp = if(obj.sale_price == null) 0.0 else obj.sale_price
            MFRawData(obj.scheme_code, obj.scheme_name, obj.net_asset_value, rp, sp, obj.date)
        }

        val diffData = correctedData.map {
          obj =>
            MFDiffData(obj.scheme_code, obj.scheme_name, obj.net_asset_value, obj.repurchase_price, obj.sale_price, obj.date, (obj.sale_price - obj.repurchase_price))
        }
        diffData.count()
    }*/
  }

  def dailyDifference(): UserDefinedFunction = udf((repurchasePrice: Double, salePrice: Double) => {
    salePrice - repurchasePrice
  })

  def convertToMFRawDataObject(line: String): MFRawData = {
    val lineArray = line.split(";")
    MFRawData(lineArray(0).toInt, lineArray(1), lineArray(2).toDouble, lineArray(3).toDouble, lineArray(4).toDouble, lineArray(5))
  }
}
