import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{FileSplit, InputSplit, TextInputFormat}
import org.apache.spark.rdd.{HadoopRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object DataFilter {

  private val prefixBlacklist: Set[String] = Set("special:", "media:", "talk:", "user:", "user_talk:", "wikipedia:",
    "wikipedia_talk:", "file:", "timedtext:", "file_talk:", "timedtext_talk:", "mediawiki:", "mediawiki_talk:",
    "template:", "template_talk:", "help:", "help_talk:", "category:", "category_talk:", "portal:", "portal_talk:",
    "topic:", "book:", "book_talk:", "draft:", "draft_talk:", "module:", "gadget:", "module_talk:", "gadget_talk:",
    "education_program:", "gadget_definition:", "education_program_talk:", "gadget_definition_talk:")

  private val filenameExtensionBlacklist: Set[String] = Set(".png", ".gif", ".jpg", ".jpeg", ".tiff", ".tif",
    ".xcf", ".mid", ".ogg", ".ogv", ".svg", ".djvu", ".oga", ".flac", ".opus", ".wav", ".webm", ".ico", ".txt",
    "_(disambiguation)")

  private val specialPages: Set[String] = Set("404.php", "Main_Page", "-")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    /**
      * When Spark reads a file from HDFS (or WASB on HDInsight),
      * a partition is created for each input split.
      *
      * In this task, each input split is exactly one hourly worth of the pageview data.
      *
      * We provide you with the code to parse the raw data into the format of
      * RDD[(date: String, line: String)], which is an RDD of all lines in the entire dataset with its date.
      *
      * For example, the line "en Kobe_Bryant 168 0" in the file "pageviews-20180308-000000"
      * will be parsed to
      * ("20180308": String, "en Kobe_Bryant 168 0": String)
      *
      * What you need to do is use proper RDD transformations and actions to
      * generate the exact same result of the MapReduce program you developed in Project 1.
      *
      * You do not have to sort the output within the Spark program,
      * the submitter will take care of it.
      */
    val test = false
    val path = if (test) "wasb://wikipediatraf@cmuccpublicdatasets.blob.core.windows.net/sample-data/" else "wasb://wikipediatraf@cmuccpublicdatasets.blob.core.windows.net/2018-march-madness/"
    val fileRDD = sc.hadoopFile[LongWritable, Text, TextInputFormat](path)
    val hadoopRDD = fileRDD.asInstanceOf[HadoopRDD[LongWritable, Text]]
    val data: RDD[(String, String)] = hadoopRDD.mapPartitionsWithInputSplit((inputSplit: InputSplit, iterator: Iterator[(LongWritable, Text)]) => {
      val filepath = inputSplit.asInstanceOf[FileSplit].getPath
      val splits = filepath.toString.split("-")
      val date = splits(splits.length - 2)
      for (item <- iterator) yield (date, item._2.toString)
    })

    /**
      * Implement the function marked with "To be implemented" to finish the following
      * filter rules.
      *
      * 1. Remove malformed lines that don't have four columns;
      * 2. Percent decoding (you can use the `PercentDecoder.scala`);
      * 3. Remove the rows if the first column is not "en" or "en.m";
      * 4. Remove the titles with invalid namespaces;
      * 5. Filter out all page titles that start with lowercase English characters;
      * 6. Filter out page titles with miscellaneous filename extensions;
      * 7. Filter out all disambiguation pages with the suffix "_(disambiguation)";
      * 8. Remove special pages ("404.php", "Main_Page", "-");
      * Feel free to go to Project 1 for more details about the filter.
      *
      * Replace the `???` with proper code to output the page views for all dates in
      * a single line in the format as below,
      * 1. Each line must have 30 days, even if there were zero page views for any particular day;
      * 2. Output articles that have over 100,000 page-views (100,000 excluded);
      * 3. The daily views should be sorted in chronological order.
      */
    val decodedOutputs = data.map(mapPercentDecode)
    val intermediateOutput: RDD[(String, Array[String])] = decodedOutputs.filter(x => filterLength(x) && filterDomain(x) && filterPrefix(x)
      && filterSuffix(x) && filterSpecialPage(x) && filterFirstLetter(x))

    /**
      * Aggregate intermediateOutput, similar to how the reducer aggregated intermediate K,V pairs
      * into the final output in the MapReduce program.
      */
    val output: RDD[String] = ???

    /**
      * The output path should be `wasb:///filter-output`.
      * This path should not exist.
      */
    output.saveAsTextFile("wasb:///filter-output")
    sc.stop()
  }

  /**
    * Perform percent-decoding and split the record into columns,
    * separated by single or consecutive whitespaces.
    *
    * We pre-implemented this method for you to help you follow and
    * learn how to perform test-driven development.
    *
    * @param x (date, record) where record refers to the pageview record
    * @return (date, columns)
    */
  def mapPercentDecode(x: (String, String)): (String, Array[String]) = {
    (x._1, PercentDecoder.decode(x._2).split("\\s+"))
  }

  /**
    * Check if column length equals to 4, which can later be used for
    * the RDD filter.
    *
    * @param x (date, columns)
    * @return true if length equals to 4
    */
  def filterLength(x: (String, Array[String])): Boolean = {
    if (x._2.size == 4) true else false
  }

  /**
    * Check if the domain code is en or en.m (case sensitive)
    *
    * @param x (date, columns)
    * @return true if the domain code is en or en.m
    */
  def filterDomain(x: (String, Array[String])): Boolean = {
    if (x._2(0) == "en" || x._2(0) == "en.m") true else false
  }

  /**
    * Check if the title starts with any blacklisted prefix, case
    * insensitive.
    *
    * @param x (date, columns)
    * @return true if the title doesn't start with any blacklisted
    *         prefix
    */
  def filterPrefix(x: (String, Array[String])): Boolean = {
    val title = x._2(1).toLowerCase
    prefixBlacklist.exists(prefix => title.startsWith(prefix))
  }

  /**
    * Check if the title ends with any blacklisted suffix, case
    * insensitive.
    *
    * @param x (date, columns)
    * @return true if the title doesn't end with any blacklisted suffix
    */
  def filterSuffix(x: (String, Array[String])): Boolean = {
    val title = x._2(1).toLowerCase
    !filenameExtensionBlacklist.exists(suffix => title.endsWith(suffix))
  }

  /**
    * Check if the title is any special page, case sensitive.
    *
    * @param x (date, columns)
    * @return true if the title isn't any special page
    */
  def filterSpecialPage(x: (String, Array[String])): Boolean = {
    !specialPages.exists(page => x._2(1).equals(page))
  }

  /**
    * Check if the first letter of the title is an English lowercase
    * letter.
    *
    * Many other Unicode characters are lowercase too.
    * Only [a-z] should count.
    *
    * @param x (date, columns)
    * @return true if the first letter of the title is not an English
    *         lowercase letter
    */
  def filterFirstLetter(x: (String, Array[String])): Boolean = {
    val firstLetter = x._2(1)(0)
    if (firstLetter >= 'a' || firstLetter <= 'z') true else false
  }

}
