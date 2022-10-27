package com.ceiproject.org

object Main {

  //creating spark session object
  //  val conf = new SparkConf().setMaster("local[*]").setAppName("Movie Ratings Application")
  //  val spark = SparkSession.builder().config(conf).getOrCreate()
  def main(args: Array[String]): Unit = {

  val uData = "file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/MovieLens Data/u.data"
  val uGenre = "file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/MovieLens Data/u.genre"
  val uItem = "file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/MovieLens Data/u.item"

    val obj = new MovieRatingsUpdate()

    obj.topMovieByRatingAndGenre(uData:String,uGenre:String,uItem:String)



  }



}
