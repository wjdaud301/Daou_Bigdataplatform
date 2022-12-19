package com.daou.analyzeasset.analyzer

import org.joda.time.{DateTime}
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import com.daou.analyzeasset.Analyzer.log


object AssetAnalyze {
  private def start_day_lit = {
    val today = DateTime.now().minusDays(7)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    today.toString(fmt)
  }

   private val day_type = "week"

   def anlAssetPrefer(sqlContext: SQLContext,df : DataFrame):DataFrame={
      import sqlContext.implicits._
      val res = df.withColumn("diff_hour", (unix_timestamp($"endTime")-unix_timestamp($"startTime"))/3600)
                  .groupBy($"assetName", $"assetId",$"itemName",$"itemId").agg(sum($"diff_hour").as("shareHour"))
                  .withColumn("type", lit(day_type))
                  .withColumn("date", lit(start_day_lit).cast("date"))        
      res
   }
  
   def anlDayPrefer(sqlContext: SQLContext,df: DataFrame):DataFrame = {
      import sqlContext.implicits._
      val res = df.withColumn("diff_hour", (unix_timestamp($"endTime")-unix_timestamp($"startTime"))/3600)
                  .withColumn("day", date_format($"startTime", "EEEE"))
                  .groupBy($"day",$"assetName",$"assetId",$"itemName",$"itemId").agg(sum($"diff_hour").as("shareTime"))
                  .withColumn("type", lit(day_type))
                  .withColumn("date", lit(start_day_lit).cast("date"))
         
      res
   }

   def anlDeptCnt(sqlContext: SQLContext,df:DataFrame, deptInfoDF : DataFrame):DataFrame = {
      import sqlContext.implicits._
      val deptCnt = deptInfoDF.groupBy($"deptId").agg(count($"userId").as("cmpr"))
      df.select($"userId")
                  .join(deptInfoDF,Seq("userId"), "left")
                  .filter($"deptName".isNotNull)
                  .groupBy($"deptName",$"deptId",$"deptPath").count()
                  .join(deptCnt, Seq("deptId"), "left")
                  .withColumn("cmpr", $"count"/$"cmpr")
                  .withColumn("type", lit(day_type))
                  .withColumn("date", lit(start_day_lit).cast("date"))
   }
   
   def anlPosCount(sqlContext: SQLContext,df:DataFrame, deptInfoDF : DataFrame):DataFrame ={
      import sqlContext.implicits._
      val posCount = deptInfoDF.groupBy($"position").agg(count($"userId").as("cmpr"))
      df.select($"position")
                  .filter($"position" =!= "")
                  .filter($"position".isNotNull)
                  .groupBy($"position").count()
                  .join(posCount, Seq("position"))
                  .withColumn("cmpr", $"count"/$"cmpr")
                  .withColumn("type", lit(day_type))
                  .withColumn("date", lit(start_day_lit).cast("date"))
   }

   def anlRate(sqlContext: SQLContext,df:DataFrame):DataFrame={
      import org.apache.spark.sql.functions.round
      import sqlContext.implicits._
      val res = df.withColumn("diff_hour", (unix_timestamp($"endTime")-unix_timestamp($"startTime"))/3600)
                           .groupBy($"assetName", $"assetId",$"itemName",$"itemId",$"startTime").agg(sum($"diff_hour").as("shareHour"))
                           .withColumn("assetRate", when($"shareHour" < 13, ($"shareHour"/12)*100).otherwise(100))
                           .groupBy($"assetName", $"assetId",$"itemName",$"itemId").agg(avg($"assetRate").as("assetRate"))
                           .withColumn("assetRate", round(col("assetRate") ,1))
                           .withColumn("type", lit(day_type))
                           .withColumn("date", lit(start_day_lit).cast("date"))
   
      res
   }
  
}
