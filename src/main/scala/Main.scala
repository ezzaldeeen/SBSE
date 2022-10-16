import org.apache.hadoop.mapreduce.lib.input.InvalidInputException
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.io.Directory
import java.io.File

object Main {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
      .setAppName("Dummy Search Engine")
      .setMaster("local[*]")
    val sparkContext = SparkContext.getOrCreate(sparkConf)
    sparkContext.setLogLevel("ERROR")

    val COLLECTION_ABS_PATH = args(0)
    val CONSTRUCTED_INV_INDEX_ABS_PATH = "./wholeInvertedIndex"

    try {
      // since we want to rebuild new inverted index
      // we have to delete the old one
      if (Directory(CONSTRUCTED_INV_INDEX_ABS_PATH).exists) {
        val directory = new Directory(
          new File(CONSTRUCTED_INV_INDEX_ABS_PATH)
        )
        directory.deleteRecursively()
      }

      // load all documents in the collection e.g. path/to/collection/*
      val collection = sparkContext
        .wholeTextFiles(COLLECTION_ABS_PATH)
        .flatMapValues(line => line.split(" "))
        .map(normalizeRecords)
        .reduceByKey(reduceTerms)
        .sortByKey()
        .map(rec => rec._1 + "," + rec._2._1 + "," + rec._2._2)
        .coalesce(1)
        .saveAsTextFile(CONSTRUCTED_INV_INDEX_ABS_PATH)
    } catch {
      case _: InvalidInputException =>
        println(s"Directory with path: '${args(0)}', is invalid.")
        return
    }

    val invertedIndex = sparkContext
      // the first partition which is the only one since the 'coalesce' sets to 1
      .textFile(CONSTRUCTED_INV_INDEX_ABS_PATH + "/part-00000")
      .map(extractTermCountDocIds)


    // -------- TRY YOUR QUERY --------
    // execute search query
    // over the inverted index
    search("to", invertedIndex)
  }

  def search(query: String, invertedIndex: RDD[(String, Array[String])]): Unit = {
    /* Process search query in order to get the relevant documents
    * through the intersections between the postings-lists for the terms */

    val tokens: Array[String] = query.split(" ")
    // the posting-list for the all terms in the query
    var postingLists: List[Array[String]] = List()

    for (token <- tokens)
      postingLists ::= invertedIndex.lookup(token).flatten.toArray
    // get the intersection between posting-lists
    val queryResult: Array[String] = postingLists.reduce((a, b) => a intersect b)

    println("QUERY RESULT:")
    if (queryResult.length == 0)
      println("There's no relevant documents for this query :(")
    else
      println(tokens.mkString("", " ", ": ") + queryResult.mkString("", ", ", ""))
  }

  def extractTermCountDocIds(line: String): (String, Array[String]) = {
    /* Extract the distinct terms, their counts, and their postingList
    * (i.e. where did these terms occur in the documents */

    // term, count, doc1, doc1, ..., docN
    val splitted = line.split(",")
    val term: String = splitted(0)
    val count: Int = splitted(1).toInt
    val docIds: Array[String] = splitted.slice(2, count + 2)

    (term, docIds)
  }

  def normalizeRecords(record: (String, String)): (String, (Int, String)) = {
    /* Dummy normalization in order to clean the splitted tokens
    * and the file names (documents) through drop noisy character,
    * and convert the given strings to lower case. */

    // some predefined noisy characters must be removed from the given token
    val noisyCharsToRemove = Map(',' -> ',', '.' -> '.',
                                  '"' -> '"', "" -> "",
                                  '?' -> '?', "!" -> "!",
                                  '\n' -> '\n')
    val cleanDocId = record._1
      .split("/").last.split('.')(0)
      .toLowerCase()
    val cleanTerm = record._2
      .filterNot(ch => noisyCharsToRemove.contains(ch))
      .toLowerCase()
    (cleanTerm, (1, cleanDocId))
  }

  def reduceTerms(firstRec: (Int, String),
                  secondRec: (Int, String)): (Int, String) = {
    /* Merge the similar terms with their count,
     and the document IDs for that term */

    // get rid of the duplication of document IDs
    if (firstRec._2.contains(secondRec._2)) {
      (firstRec._1, firstRec._2)
    } else {
      (firstRec._1 + secondRec._1, firstRec._2 + "," + secondRec._2)
    }
  }
}