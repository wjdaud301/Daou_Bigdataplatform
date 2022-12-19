package com.daou.analyzeboard.db

import java.util.Properties

import com.mongodb.spark.MongoSpark
import org.bson.Document  
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

object DbManager {  
  def writeToMongoDB(d: DataFrame, collectionName: String) {
    MongoSpark.save(d.write.option("collection", collectionName).mode("append"))
  } 

  def readMongo(sqlContext: SQLContext): DataFrame = {
    val cmprApvlSpeed = sqlContext.read.format("mongo")
                .option("spark.mongodb.input.uri",
                      "mongodb://bigdata:1q2w#E$R@ahe-common01:27017/DaouDB.infoBoardPost")
                .load().drop("_id","date")
    cmprApvlSpeed
  }
}

