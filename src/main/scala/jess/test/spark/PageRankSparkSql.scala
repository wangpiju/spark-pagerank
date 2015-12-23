package jess.test.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.util.Calendar


class PageSql(val title: String, val links: Array[String]) {}

object PageRankSparkSql {
  // Parse an input line of the form "mid, uid"
  def parsePage(line: String): PageSql = {
		val pieces = line.split(",")
		val title = pieces(0)
		val links = pieces.tail.map(_.trim())
		new PageSql(title, links)
  }
	
  def main(args: Array[String]){
    val start = java.lang.System.currentTimeMillis()
    println("start: " + start)
    var conf = new SparkConf().setAppName("WordCount").setMaster("local")
    var sc = new SparkContext(conf)
    var sqlc =  new org.apache.spark.sql.SQLContext(sc)
    val csvAll = sqlc.load("com.databricks.spark.csv", Map("path" -> "part-of-week1.csv", "header" -> "true"))
    csvAll.registerTempTable("AllData")
    val result = sqlc.sql("select mid,mid,uid from AllData").rdd.map(row => row.toString.substring(1, row.length - 2))
    
    //result.foreach { println }
    
    val pages = result.map(parsePage)
		
		val links = pages.map(p => (p.title, p.links))	//RDD[(String, Array[String])]
		var ranks = pages.map(p => (p.title, 1.0))	//RDD[(String, Double)]
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