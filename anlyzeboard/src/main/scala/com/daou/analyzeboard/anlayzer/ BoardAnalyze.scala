package com.daou.analyzeboard.analyzer

import org.joda.time.{DateTime}
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import com.daou.analyzeboard.Analyzer.log


object BoardAnalyze {

  def anlPostCnt(sqlContext: SQLContext,targetDF : DataFrame, postDF: DataFrame, actionCategory: String):DataFrame ={
      import sqlContext.implicits._
      // READ_CONTENT, CREATE_CONTENT, RECOMMEND
      val res = targetDF.filter(col("action") === actionCategory)
                  .join(postDF, Seq("postId"), "left")
                  .groupBy(col("postId"),col("title"),col("type"),col("date"))
                  .count()
                  
      res
   }

   def anlUserCnt(sqlContext: SQLContext,targetDF : DataFrame, actionCategory: String):DataFrame ={
      val res = targetDF.filter(col("action") === actionCategory) // READ_CONTENT, CREATE_CONTENT, RECOMMEND
                  .groupBy(col("userId"),col("userName"),col("position"), col("deptName"),col("deptId"), col("deptPath"),col("type"),col("date"))
                  .count()
      res
   }

   def anlPosCnt(sqlContext: SQLContext, targetDF : DataFrame, userInfo : DataFrame, actionCategory: String):DataFrame ={
      val posCnt = userInfo.select(col("userId"), col("position")).distinct().groupBy(col("position")).agg(count("*").alias("cmpr")) 
      val res = targetDF.filter(col("action") === actionCategory) // READ_CONTENT, CREATE_CONTENT, RECOMMEND
                  .filter(col("position") =!= "")
                  .filter(col("position").isNotNull)
                  .groupBy(col("position"),col("type"),col("date")).count()
                  .join(posCnt, Seq("position"), "left")
                  .withColumn("cmpr", col("count")/col("cmpr"))
                  
      res
   }

   def anlDeptCnt(sqlContext: SQLContext, targetDF : DataFrame, userInfo: DataFrame, actionCategory: String):DataFrame ={
      val deptCnt = userInfo.groupBy(col("deptName")).agg(count("*").alias("cmpr"))
      val res = targetDF.filter(col("action") === actionCategory) // READ_CONTENT, CREATE_CONTENT, RECOMMEND
                  .filter(col("deptName").isNotNull)
                  .groupBy(col("deptName"),col("deptId"),col("deptPath"),col("type"),col("date")).count()
                  .join(deptCnt, Seq("deptName"), "left")
                  .withColumn("cmpr", col("count")/col("cmpr"))
      res
   }
}