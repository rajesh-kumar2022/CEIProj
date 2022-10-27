package com.ceiproject.org

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, rank, sum}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

class MovieRatingsUpdate extends MovieRating {

  val conf = new SparkConf().setMaster("local[*]").setAppName("Movie Ratings Application")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  override def topMovieByRatingAndGenre(a:String,b:String,c:String) = {
println("++++++INSIDE topMovieByRatingAndGenre METHOD+++++++++++++++++++++++++++++++++++")
    val uData = spark.sparkContext.textFile(a)  //.map(x => x.split(" ")).filter(x => !(x.isEmpty)).map(att => Row(att(0),att(1),att(2),att(3)))
    val uGenre = spark.sparkContext.textFile(b)
    val uItem = spark.sparkContext.textFile(c)
    println("COUNT OF THE FILE"+uData.count())

    //uData.collect()
    val splitRDD = uData.map(x => {
    var f = x.map(a => a)
    var cc =  f.split(" ").filter(x => !(x.isEmpty))

     cc
    }).map(att => Row(att(0).trim,att(1).trim,att(2).trim,att(3).trim))


    val uItemRDD = uItem.map(line => line.split("|")).map(att => Row(att(0),att(1),att(2),att(3),att(4),att(5),att(6),att(7),att(8),att(9),att(10),att(11),att(12),att(13),att(14),att(15),att(16),att(17),att(18),att(19),att(20),att(21),att(22),att(23)))
    //creating schema for uData


    val uData_Schema =  StructType(Array(
    StructField("user_id",IntegerType),
    StructField("item_id",IntegerType),
    StructField("rating",IntegerType),
    StructField("timestamp",StringType)
    ))

    val uItemSchema = StructType(Array(
      StructField("movie_id", IntegerType),
        StructField("movie_title",StringType),
        StructField("release_date", StringType),
        StructField("video_release_date",StringType),
        StructField("imdb_url",StringType),
        StructField("unknown",IntegerType),
        StructField("action",IntegerType),
        StructField("adventure",IntegerType),
        StructField("animation",IntegerType),
        StructField("childrens",IntegerType),
        StructField("comedy",IntegerType),
        StructField("crime",IntegerType),
        StructField("documentary",IntegerType),
        StructField("drama",IntegerType),
        StructField("fantasy",IntegerType),
        StructField("film_noir",IntegerType),
        StructField("horror",IntegerType),
        StructField("musical",IntegerType),
        StructField("mystery",IntegerType),
        StructField("romance",IntegerType),
        StructField("sci_fi",IntegerType),
        StructField("thriller",IntegerType),
        StructField("war",IntegerType),
        StructField("western",IntegerType))
      )


    val uDataDF = spark.createDataFrame(splitRDD,uData_Schema)
    val uItemDF = spark.createDataFrame(uItemRDD,uItemSchema)

    val join_DF = uDataDF.join(uItemDF,uDataDF("item_id")===uItemDF("movie_id"),"inner")
    val newDF = join_DF.withColumn("genre_value",col("unknown")+col("action")+col("adventure")+col("animation")+col("childrens")+col("comedy")+col("crime")+col("documentary")+col("drama")+col("fantasy")+col("film_noir")+col("horror")+col("musical")+col("mystery")+col("romance")+col("sci_fi")+col("thriller")+col("war")+col("western"))

    val win=Window.partitionBy(col("genre_value")).orderBy(col("rating").desc)

    val topMovies = newDF.withColumn("Top_Movies",rank().over(win))

    val topMoviesDF=  topMovies.where(col("Top_Movies") <= 10).select(col("*"))

    topMoviesDF.write.format("parquet").mode("Overwrite").save("file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/topMovies")

    newDF.createOrReplaceGlobalTempView("newDF_vw")

    val twoYearDataDF = spark.sql(s"""select * from newDF_vw where release_date >= "01-Jan-1995" and release_date < "01-Jan-1997"""")

    val win1 = Window.partitionBy(col("release_date")).orderBy(col("rating").desc)

    val win2 = Window.partitionBy(col("release_date")).orderBy(col("rating").asc)

    val twoYearDataDF1 = twoYearDataDF.withColumn("rnk_mvi",rank().over(win1))
    val twoYearDataDF2 = twoYearDataDF.withColumn("rnk_mvi",rank().over(win2))
    //top movies with in 2 years
   val topMovie= twoYearDataDF1.where(col("rnk_mvi")===1).select(col("*"))


    topMovie.write.format("parquet").mode("Overwrite").save("file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/topMovies_Two_Year")
//least popular movie
val topMovie1= twoYearDataDF2.where(col("rnk_mvi")===1).select(col("*"))
    topMovie1.write.format("parquet").mode("Overwrite").save("file:///Users/sai kumar/Documents/Practice/CEI_Prac/MovieLens Data/leastMovies_Two_Year")



  }

//  override def mostPopularAndLeasePopular: Unit = {
//  }
//
//  override def monthlyTrendRatings: Unit = {
//
//  }

}
