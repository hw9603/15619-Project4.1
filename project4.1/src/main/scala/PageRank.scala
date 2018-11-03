import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions


object PageRank {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
          .builder
          .appName("PageRank")
          .getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext

        val iters = 10
        val lines = sc.textFile("wasb://spark@cmuccpublicdatasets.blob.core.windows.net/Graph")

        val followees = lines.map(s => s.split("\t")(1))
        val followers = lines.map(s => s.split("\t")(0))
        // find the dangling users
        val dangling = followees.subtract(followers).distinct().map(user => (user, "na"))

        // Combine dangling and non-dangling users
        val links = lines.map{ s =>
          val follows = s.split("\t")
          (follows(0), follows(1))
        }.union(dangling).groupByKey().cache()

        // the number of total nodes
        val nodes = 1006458
        // initialize the ranks to be 1/N
        var ranks = links.mapValues(v => (1.0 / nodes))

        for (i <- 1 to iters) {
          var dang = sc.accumulator(0.0)
          val rankedLink = links.join(ranks).values
          // Compute the contribution of all dangling users
          rankedLink.foreach{
            case (followeeArr, rank) =>
              if (followeeArr.exists(x => x == "na")) {
                dang.add(rank)
              }
          }
          // Compute non-dangling users' contribution
          val contribs = rankedLink.flatMap{
            case (followeeArr, rank) =>
              val numNeighbors = followeeArr.size
              if (!followeeArr.exists(x => x == "na")) {
                followeeArr.map(followee => (followee, rank / numNeighbors))
              } else {
                List()
              }
          }
          val dangVal = dang.value
          ranks = contribs.reduceByKey(_ + _).mapValues(contrib => 0.15 / nodes + 0.85 * (contrib + dangVal / nodes))
        }

        val output = ranks.map{case (userid, rank) => userid + "\t" + rank}
        output.saveAsTextFile("wasb:///pagerank-output")

        spark.stop()
    }
}
