import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object Main {
  def main(args: Array[String]) {
    val input = "test.txt"
    val sc = new SparkContext("local", "Word Count", "~/spark-1.2.0", List("target/scala-2.11/word-count_2.11-1.0.jar"))

    val data = sc.textFile(input, 2).cache()
    val tokens = sc.textFile(input, 2).flatMap(line => line.split(" "))
    val freq = tokens.map(word => (word.toLowerCase(), 1)).reduceByKey((a, b) => a + b)

    freq.collect.map(tf => println("Term, Frequency: " + tf))
    tokens.saveAsTextFile("tokens")
    freq.saveAsTextFile("term_freq")
  }
}
