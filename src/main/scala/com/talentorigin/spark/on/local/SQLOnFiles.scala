package com.talentorigin.spark.on.local

import org.apache.spark.sql.SparkSession

object SQLOnFiles {
  def main(args: Array[String]): Unit = {
    val fileName = "/Users/talentorigin/workspace/datasets/mutualfunds/raw_mf_data/consolidated_mf_data.csv"
    val spark = SparkSession.builder()
      .appName("Spark RDDs")
      .config("spark.sql.sources.default","csv")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val count = spark.sql(s"select count(*) from csv.`$fileName`")
    count.show()
  }
}
