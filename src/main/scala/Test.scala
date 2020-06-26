import com.yiyuan.function.cusLoader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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



    //metadataPlaceholder serves as placeholder for metadata retrieved from data
    val loader = new cusLoader(Spark)
    val ISSDF = loader.load("ISSLogfileedit.txt", "com.yiyuan.customdsClasses.datasourceCusloader1")
    ISSDF.show(10)
    ISSDF.printSchema()

    val swimDF = loader.load("swimming.csv", "com.yiyuan.customdsClasses.datasourceCusloader2")
    swimDF.show(20)
    swimDF.printSchema()


    //Spark.conf.set serve as placeholder for metadata retrieved from data as well
    //Spark.conf.set does not overwrite. To try one, comment the others.
    /*try {
      Spark.conf.set("metadata key","com.yiyuan.customdsClasses.datasourceISSLog")
      val cusLoad = Spark.read.format("yySource")
          .load("ISSLogfileedit.txt")
      cusLoad.show(10)
      cusLoad.printSchema()
    } catch {
      case _ : Throwable => println("Load failed")
    }*/

    /*try {
      Spark.conf.set("metadata key","com.yiyuan.customdsClasses.datasourceSwimD")
      val cusLoad = Spark.read.format("yySource")
        .load("swimming.csv")
      cusLoad.show(10)
      cusLoad.printSchema()
    } catch {
      case _ : Throwable => println("Load failed")
    }*/

    try {
      Spark.conf.set("metadata key","com.yiyuan.customdsClasses.datasourceTestTxt")
      val cusLoad = Spark.read.format("yySource")
      .load("TwoFieldTextFile.txt")
      cusLoad.show(10)
      cusLoad.printSchema()
    } catch {
    case _ : Throwable => println("Load failed")
    }
  }
}
