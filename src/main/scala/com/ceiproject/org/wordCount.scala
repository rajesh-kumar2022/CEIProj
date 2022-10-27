package com.ceiproject.org

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object wordCount extends App {
  //creating spark conf and session object
  val conf = new SparkConf().setAppName("Word Count Program").setMaster("local[*]")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  //reading file

  val ip_file = spark.sparkContext.textFile("file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/MovieLens Data/README")
  //splitting the file based on space
  val words = ip_file.flatMap(x => x.split(" ")).map(x => (x, 1))

  //count the words

  val cntWord = words.reduceByKey(_ + _)
  cntWord.collect()

  cntWord.saveAsTextFile("file:///Users/sai kumar/Documents/Practice/CEI_Prac/wordCount")

}
