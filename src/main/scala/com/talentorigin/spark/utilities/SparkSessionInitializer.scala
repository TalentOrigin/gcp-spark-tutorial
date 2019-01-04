package com.talentorigin.spark.utilities

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSessionInitializer {
  var sparkSession: SparkSession = _
  var sparkConf: SparkConf = _
  def getSparkSession(master: String = "local", appName: String = "Spark Job"): SparkSession = {
    if (sparkSession == null) sparkSession = SparkSession.builder()
      .master(master)
      .appName(appName)
      .enableHiveSupport()
      .getOrCreate()
    sparkSession
  }

  def getSparkConf(master: String = "local", appName: String = "Spark Job"): SparkConf = {
    if(sparkConf == null) sparkConf = new SparkConf()
      .setMaster(master)
      .setAppName(appName)

    sparkConf
  }
}
