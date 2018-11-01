import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object FollowerRDD {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf()
        val sc = new SparkContext(conf)

        val graphRDD = sc.textFile("wasb://spark@cmuccpublicdatasets.blob.core.windows.net/Graph")
        val output: RDD[String] = graphRDD.distinct()
            .map(line => (line.split("\t")(1), 1))
            .reduceByKey(_ + _)
            .sortBy(_._2)
            .take(100)
            .map{
                case (userid, count) =>
                    userid + "\t" + count
            }

        output.saveAsTextFile("wasb:///followerRDD-output")
        sc.stop()
    }
}