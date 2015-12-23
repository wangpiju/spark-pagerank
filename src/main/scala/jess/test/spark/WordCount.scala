package jess.test.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext


object WordCount {
  def main(args: Array[String]){
    var conf = new SparkConf().setAppName("WordCount").setMaster("local")
    var sc = new SparkContext(conf)
    
    var test = sc.textFile("food.txt")
    test.flatMap (line => line.split(" ") ).map ( word => 
    (word, 1) )
    .reduceByKey(_ + _)
    .saveAsTextFile("food.count.txt")
    
  }
  
}