import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, Row}


object FollowerDF {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder
          .appName("Twitter ETL")
          .getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        val graphDF = sc.textFile("wasb://spark@cmuccpublicdatasets.blob.core.windows.net/Graph")
                         .map(line => line.split("\t")).toDF("follower", "followee")

        val df = graphDF.distinct().groupBy("followee").count().limit(100)

        df.write.format("parquet").save("wasb:///followerDF-output")
        sc.stop()
    }
}