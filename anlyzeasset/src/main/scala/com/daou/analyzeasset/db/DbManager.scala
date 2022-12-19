package com.daou.analyzeasset.db

import java.util.Properties

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.DataFrame

import org.joda.time.{DateTime}
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object DbManager {  

  private def  start_date= {
    val target_date = DateTime.now().minusDays(7)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val time = "ISODate('" + target_date.toString(fmt) + "T15:00:00Z')"
    time
  }

  private def end_date = {
    val cmprdate = DateTime.now()
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val time = "ISODate('" + cmprdate.toString(fmt) + "T15:00:00Z')"
    time
  }

  // private val pipeline = "{'$match': { $or : [ {'startDay': {'$gte' : {'$date' : " + start_date + " }, '$lte' : {'$date' : "+ end_date+ " }}}, {'endDay': {'$gte' : {'$date' : " + start_date + " }, '$lte' : {'$date' : "+ end_date+ " }}}]} }"  // 이상, 이하
  
  private val pipeline = "{ '$match': {'$or': [{'startDay': {'$gte' : " + start_date + " , '$lte' : " + end_date + " }}, {'endDay': { '$gte' : "+ start_date+ ", '$lte' : " + end_date + " }}] }}"

  def writeToMongoDB(d: DataFrame, collectionName: String) {
    MongoSpark.save(d.write.option("collection", collectionName).mode("append"))
  } 

  def readMongo(sqlContext: SQLContext, userInfo: DataFrame):DataFrame = {
    import sqlContext.implicits._
    val posDF = userInfo.select($"userId", $"position").distinct()//.groupBy(col("position")).agg(count("*").alias("count")) 

    val assetMongoDF = sqlContext.read.format("mongo")
                .option("spark.mongodb.input.uri",
                      "mongodb://bigdata:1q2w#E$R@proah-common01:27017/DaouDB.infoAssetStatus")
                .option("pipeline", pipeline)
                .load()
                .filter($"delFlag" === 0)
                .filter($"startDay" === $"endDay")
                .drop("delFlag", "startDay", "endDay", "_id")
                .join(posDF, Seq("userId"))

    assetMongoDF  
  }
}

