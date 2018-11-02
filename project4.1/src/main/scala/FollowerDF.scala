import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, Row}


object FollowerDF {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder
          .appName("task1")
          .getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        // val graphRDD = sc.textFile("wasb://spark@cmuccpublicdatasets.blob.core.windows.net/Graph")
        val graphRDD = sc.textFile("file:///Users/hewen/Stuffs/Fall2018/15-619/15619-Project4.1/graph")

        val graphDF = graphRDD.map{
          line =>
            val follows = line.split("\t")
            (follows(0), follows(1))
        }.toDF("follower", "followee")

        val df = graphDF.distinct().groupBy("followee").count().limit(100)

        // df.write.format("parquet").save("wasb:///followerDF-output")
        df.write.format("parquet").save("file:///Users/hewen/Stuffs/Fall2018/15-619/15619-Project4.1/followerDF-output")
        sc.stop()
    }
}