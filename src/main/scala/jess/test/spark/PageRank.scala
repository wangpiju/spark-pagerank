package jess.test.spark
import org.apache.spark.SparkConf
import java.util.Calendar
import org.apache.spark.SparkContext

class Page(val title: String, val links: Array[String]) {}

object PageRank {
  // Parse an input line of the form "mid, uid"
	def parsePage(line: String): Page = {
		val pieces = line.split(",")
		val title = pieces(0)
		val links = pieces.tail.map(_.trim())
		new Page(title, links)
	}
	
  def main(args: Array[String]){
     val start = java.lang.System.currentTimeMillis()
    println("start: " + start)
    var conf = new SparkConf().setAppName("WordCount").setMaster("local")
    var sc = new SparkContext(conf)
    val csv = sc.textFile("part-of-week1.csv")
    // split / clean data
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    // get header
    val header = headerAndRows.first
    // filter out header (eh. just check if the first val matches the first header name)
    val data = headerAndRows.filter(_(0) != header(0))
    // splits to map (header/value pairs)
    val maps = data.map(splits => header.zip(splits).toMap)
    // filter out the user "me"
    val result = maps.map(map => map("mid") + "," + map("mid") + "," + map("uid") )
    //result.foreach { println }
    val pages = result.map(parsePage)
		
		val links = pages.map(p => (p.title, p.links))	// RDD[(String, Array[String])]
		var ranks = pages.map(p => (p.title, 1.0))	// RDD[(String, Double)]
		pages.cache()
    val numIterations = 10
    
		for (iteration <- 0 until numIterations) {
			val contribs = links.join(ranks).flatMap {
				case (title, (neighbors, rank)) =>
					neighbors.map(n => (n, rank / neighbors.size))
			}
			ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
		}
    
		// print result
		println("Final ranks:")
		ranks.takeOrdered(10)(Ordering[Double].reverse.on(x=>x._2)).foreach(println)
		val end = java.lang.System.currentTimeMillis()
		println("end: " + end)
		println(end - start)
		sc.stop()
  }
  
}