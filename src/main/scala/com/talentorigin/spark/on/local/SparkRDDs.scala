package com.talentorigin.spark.on.local

/*import com.talentorigin.spark.beans.MutualFund
import com.talentorigin.spark.utilities.SparkSessionInitializer
import org.apache.spark.sql.Encoders

object SparkRDDs {
  def main(args: Array[String]): Unit = {
    val fileName = "/Users/talentorigin/workspace/datasets/mutualfunds/raw_mf_data/consolidated_mf_data.csv"
    val spark = SparkSessionInitializer.getSparkSession()

    val schema = Encoders.product[MutualFund].schema

    import spark.implicits._
    val mfDataframe = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv(fileName)
        .as(Encoders.bean[MutualFund](classOf[MutualFund]))


    mfDataframe.printSchema()
    mfDataframe.show(false)
    val groupedDataset = mfDataframe.groupBy(mfDataframe.col("schemeCode"))
  }
}*/
