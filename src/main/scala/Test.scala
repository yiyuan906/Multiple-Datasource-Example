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
    //"metadata key" set is to serve as a placeholder for the metadata that is to be read from MinIO
    //the key set cannot be overwritten, so only run one at a time as they are using the same key

    //swimming.csv example
    //data is not aligned on some line from 41-50, show(40) would be the maximum for this data
    /*Spark.conf.set("metadata key","com.yiyuan.workingClass.datasourceSwimRelation")
    val cusLoad = Spark.read.format("ySource2").load("swimming.csv")
    cusLoad.show(10)
    cusLoad.printSchema()*/

    //ISSLogfileedit.txt example
    /*Spark.conf.set("metadata key", "com.yiyuan.workingClass.datasourceISSRelation")
    val cusLoad = Spark.read.format("ySource2").load("ISSLogfileedit.txt")
    cusLoad.show(10)
    cusLoad.printSchema()*/
  }
}
