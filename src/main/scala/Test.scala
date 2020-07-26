import org.apache.spark.sql.SparkSession

object Test {
  def main(args: Array[String]): Unit = {

    val endpoint = "http://127.0.0.1:9000"
    val access_key = "minio"
    val secret_key = "minio123"

    val Spark = SparkSession.builder()
      .master("local[*]")
      .appName("Custom datasource implementation")
      .getOrCreate()

    //parquet data
    /*val dummydata = Seq(("2020-06-06","12:12:12","UPDATE","Admin","12312312","Update one")
      ,("2020-06-06","11:11:11","UPLOAD","User","11111111","upload file to open lib")
      ,("2020-06-07","10:10:10","UPDATE","Admin","12131415","Update two")
      ,("2020-06-07","15:15:15","GET","Admin","11112222","Get information from lib Z"))
    val serDdata = Spark.sparkContext.parallelize(dummydata)
    import Spark.implicits._
    val serDF = serDdata.toDF("Date","Time","Action","Role","ActionID","Description")
    serDF.coalesce(1).write.mode("overwrite").parquet("parquetfile")*/

    val cusLoad = Spark.read.format("CDS")
      .option("endpoint",endpoint)
      .option("accesskey",access_key)
      .option("secretkey",secret_key)
      //.option("readMode","specific")
      .load(
        "customdatasources/demo/"
       // "customdatasources/logsampledata/logA1.txt,customdatasources/logsampledata/logC1.parquet,customdatasources/logsampledata/logA2.txt"
      /*"customdatasources/logsampledata/logA1.txt," +
        "customdatasources/logsampledata/logB2.csv,customdatasources/logsampledata/logA2.txt," +
        "customdatasources/logsampledata/logB1.csv"*/
    )
    cusLoad.show(50)
  }
}
