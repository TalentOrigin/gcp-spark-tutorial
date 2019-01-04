import org.apache.spark.sql.SparkSession

object MyFirstSparkJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("First GCP DataProc Job")
      .getOrCreate()

    val rdd = spark.sparkContext.parallelize(Array(1,2,3,4,5))
    rdd.collect().foreach(println)

    spark.conf.getAll.foreach(println)
  }
}
