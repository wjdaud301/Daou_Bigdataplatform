package com.daou.appanalyzer.db

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import scala.util.control._


object DbManager {
  private def getCmprDate = {
    val cmprdate = DateTime.now().minusDays(15)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val time = "ISODate('" + cmprdate.toString(fmt) + "T15:00:00Z')"
    time
  }

  private def getTargetDate = {
    val target_date = DateTime.now().minusDays(8)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val time = "ISODate('" + target_date.toString(fmt) + "T15:00:00Z')"
    time
  }

  private val cmprPipeline = "{'$match':{ 'date' : {'$eq' : " + getCmprDate + " }}}"
  private val targetPipeline = "{'$match':{ 'date' : {'$eq'  : " + getTargetDate + " }}}"
  // private val pipeline = "{'$match':{'date': {'$eq' : {'$date' : " + getCmprDate + " }} }}" 

  def writeToMongoDB(d: DataFrame, collectionName: String) {
    MongoSpark.save(d.write.option("collection", collectionName).mode("append"))
  } 

  def readMongo(sqlContext: SQLContext) : (DataFrame, DataFrame) = {
    var cmprApvlSpeed = sqlContext.read.format("mongo")
                              .option("spark.mongodb.input.uri",
                                      "mongodb://bigdata:1q2w#E$R@ahe-common01:27017/DaouDB.homeApvlRank")
                              .option("pipeline", cmprPipeline)
                              .load().drop(col("_id"))

    val targetApvlSpeed = sqlContext.read.format("mongo")
                              .option("uri","mongodb://bigdata:1q2w#E$R@ahe-common01:27017/DaouDB.apvlSpeed")
                              .option("pipeline", targetPipeline)
                              .load()
                              .drop(col("_id"))
                              // .filter(col("date") === getTargetDate)

    // 만약 비어 있다면 진행
    var loop = new Breaks
    if (cmprApvlSpeed.select("*").count() == 0) {
          var idx = 15
          val reference_date = DateTime.now()
          val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
          loop.breakable {
            // 2주전부터 30일동안 데이터가 있으면 불러온다
            while (idx < 45) {
              val success_time_date = reference_date.minusDays(idx)
              val time = "ISODate('" + success_time_date.toString(fmt) + "T15:00:00Z')"
              val Pipeline = "{'$match':{ 'date' : {'$eq' : " + time + " }}}"

              val cmp_df = sqlContext.read.format("mongo")
                .option("spark.mongodb.input.uri",
                  "mongodb://bigdata:1q2w#E$R@ahe-common01:27017/DaouDB.homeApvlRank")
                .option("pipeline", Pipeline)
                .load().drop(col("_id"))

              if (cmp_df.select("*").count() != 0) {
                cmprApvlSpeed = cmp_df
                loop.break
              }
              idx += 7
            }
          }
    }


    (cmprApvlSpeed, targetApvlSpeed)
  }

}