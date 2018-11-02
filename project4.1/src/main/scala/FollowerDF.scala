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

        val graphRDD = sc.textFile("wasb://spark@cmuccpublicdatasets.blob.core.windows.net/Graph")

        // Convert the RDD to Dataframe
        val graphDF = graphRDD.map{
          line =>
            val follows = line.split("\t")
            (follows(0), follows(1))
        }.toDF("follower", "followee")

        // Use Spark SQL API to do the counting and sorting job
        val df = graphDF.groupBy("followee").count().orderBy($"count".desc).limit(100)

        df.write.format("parquet").save("wasb:///followerDF-output")

        sc.stop()
    }
}
