import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object FollowerRDD {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        val sc = new SparkContext(conf)
        val graphRDD = sc.textFile("wasb://spark@cmuccpublicdatasets.blob.core.windows.net/Graph")
        // Retrieve the followee information
        val followers = graphRDD.map(line => (line.split("\t")(1), 1))
        // Add up the followers
        val count = followers.reduceByKey(_ + _)
        // Sort by the number of followers and take the first 100 rows
        val sortCount = count.sortBy(-_._2).take(100)
        // Map to the desired output format
        val output: RDD[String] = sc.parallelize(sortCount)
                                    .map{
                                        case (userid, count) =>
                                            userid + "\t" + count
                                    }
        output.saveAsTextFile("wasb:///followerRDD-output")
        sc.stop()
    }
}
