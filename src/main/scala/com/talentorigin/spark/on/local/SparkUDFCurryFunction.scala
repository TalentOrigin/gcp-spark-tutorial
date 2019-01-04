package com.talentorigin.spark.on.local

import com.talentorigin.spark.utilities.SparkSessionInitializer
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, when}

object SparkUDFCurryFunction {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionInitializer.getSparkSession("local", "Spark UDF with Curry Functions")
    val dataLocation = args(0)

    import spark.implicits._
    val data = spark.read
      .option("inferSchema","true")
      .option("delimiter",";")
      .option("header", "true")
      .csv(dataLocation)
      .as[MFRawData]

    val correctedData = data.withColumn("repurchase_price", when(data("repurchase_price").isNull, 0.0).otherwise(data("repurchase_price")))
        .withColumn("sale_price", when(data("sale_price").isNull, 0.0).otherwise(data("sale_price")))

    val diffPriceData = correctedData.withColumn("diff_price", dailyDifference()(correctedData("repurchase_price"), correctedData("sale_price")))
    diffPriceData.show()
  }

  def dailyDifference(): UserDefinedFunction = udf((repurchasePrice: Double, salePrice: Double) => {
    salePrice - repurchasePrice
  })

}

case class MFRawData(scheme_code: Integer, scheme_name: String, net_asset_value: Double, repurchase_price: Double, sale_price: Double, date: String)

case class MFDiffData(scheme_code: Integer, scheme_name: String, net_asset_value: Double, repurchase_price: Double, sale_price: Double, date: String, diff_price: Double)