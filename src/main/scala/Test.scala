import com.yiyuan.function.cusLoader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.unix_timestamp
import java.text.SimpleDateFormat

object Test {
  def main(args: Array[String]): Unit = {

    val endpoint = "http://127.0.0.1:9000"
    val access_key = "minio"
    val secret_key = "minio123"

    val conf = new SparkConf()
      .set("spark.hadoop.fs.s3a.endpoint", endpoint)
      .set("spark.hadoop.fs.s3a.access.key", access_key)
      .set("spark.hadoop.fs.s3a.secret.key", secret_key)
      .set("spark.hadoop.fs.s3a.path.style.access", "true")
      .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val Spark = SparkSession.builder()
      .config(conf)
      .master("local[*]")
      .appName("Metadata read to schema example")
      .getOrCreate()

    //This example uses customdsProviderSecond as the custom datasource
    //To run this example, it requires MinIO to have the files with their metadata.
    //The key that can be set with MinIO does not allow a list of any sorts, so it cannot be used to run this example
    //on the ide.

    //supports single file, all files (provided same schema), file of same extension
    // and files containing certain string as well (logA_*.filetype).

    val cusLoad = Spark.read.format("ySource2").load(
      "s3a://customdatasources/logsampledata/*"
    )
    cusLoad.show(50)
    cusLoad.printSchema()
  }
}
