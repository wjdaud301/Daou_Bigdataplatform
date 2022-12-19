package com.daou.apvlanalyzer.analyzer

import org.joda.time.{DateTime}
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import com.daou.apvlanalyzer.Analyzer.log

object ApvlAnalyze {

 def start_day_lit = {
    val today = DateTime.now().minusDays(7)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    today.toString(fmt)
  }
  
//   private val day_type = "week"
  private val dateType = "week"

  def anlUseDay(sqlContext: SQLContext,targetDF : DataFrame):DataFrame ={
        val res = targetDF.groupBy(col("day"), col("type"), col("date")).count()
                      
        res
  }

  def anlFormUseDay(sqlContext: SQLContext,targetDF : DataFrame):DataFrame ={
        val res = targetDF.groupBy(col("day"), col("formName"), col("type"), col("date")).count()
                      .withColumn("type", lit(dateType))
                      
        res
  }

  def anlUserCnt(sqlContext: SQLContext,targetDF : DataFrame):DataFrame ={
        val res = targetDF.filter(col("userName") =!= "").filter(col("userName").isNotNull)
                    .groupBy(col("userId"),col("userName"),col("deptName"),col("deptId"),col("deptPath"),col("position"), col("type"), col("date"))
                    .count()
                    .withColumn("type", lit(dateType))
        res
  }

  def anlPosCnt(sqlContext: SQLContext,targetDF : DataFrame, userInfo: DataFrame):DataFrame ={
        val posCnt = userInfo.select(col("userId"), col("position")).distinct().groupBy(col("position")).agg(count("*").alias("cmpr"))   
        
        val res = targetDF.filter(col("position") =!= "").filter(col("position").isNotNull)
                    .groupBy(col("position"), col("type"), col("date")).count()
                    .join(posCnt, Seq("position"), "left")
                    .withColumn("cmpr", col("count")/col("cmpr"))
                    .withColumn("type", lit(dateType))
        res
  }

  def anlDeptCnt(sqlContext: SQLContext,targetDF : DataFrame, userInfo: DataFrame):DataFrame ={
        val deptCnt = userInfo.groupBy(col("deptName")).agg(count("*").alias("cmpr"))
        val res = targetDF.filter(col("deptName").isNotNull)
                    .groupBy(col("deptName"),col("deptId"),col("deptPath"), col("type"), col("date")).count()
                    .join(deptCnt, Seq("deptName"), "left")
                    .withColumn("cmpr", col("count")/col("cmpr"))
                    .withColumn("type", lit(dateType))
                  //   .withColumn("date", lit(start_day_lit).cast("date"))
        res
  }

  def draftCmprWdr(sqlContext: SQLContext,apvlDF : DataFrame): DataFrame ={
        val apr_compl = apvlDF.filter(col("docStatus") === "COMPLETE" )
                    .groupBy(col("targetDate"), col("docStatus")).count()
                    .withColumnRenamed("count", "complete")
        val apr_wdr = apvlDF.filter(col("docStatus") === "RETURN")
                    .groupBy(col("targetDate"), col("docStatus")).count()
                    .withColumnRenamed("count", "withdraw")
        val res = apr_compl.join(apr_wdr, Seq("targetDate"), "outer")
                    .select(col("targetDate").alias("date"), col("complete"), col("withdraw")).na.fill(0, Seq("withdraw"))
        res
  }

  def analyzeSpeed(sqlContext: SQLContext, draftDF: DataFrame, apvlDF : DataFrame): (DataFrame,DataFrame,DataFrame) ={    
        // 날짜 범위 안에 기안된 문서(draft) + 승인 완료된 문서(approval)

        val formInfo = draftDF.select(col("formName"), col("documentId")).distinct()                     
        val resUser = apvlDF
                            .withColumn("requestTime", unix_timestamp(col("time")) - unix_timestamp(col("prevCompletedAt")))
                            .select(col("userName"), col("userId"), col("position"), col("deptName"),col("deptId"),col("deptPath"),col("requestTime"))
                            .withColumn("requestTime", col("requestTime")/60)
                            .filter(col("requestTime") > 0)
                            .groupBy(col("userName"), col("userId"), col("position"), col("deptName"),col("deptId"),col("deptPath"))
                            .agg(avg(col("requestTime")).as("averageSpeed"),count(col("userName")).as("count"))
                            .withColumn("type", lit(dateType))
                            .withColumn("date", lit(start_day_lit).cast("date"))
                            
        val resForm = apvlDF
                            .withColumn("requestTime", unix_timestamp(col("time")) - unix_timestamp(col("prevCompletedAt")))
                            .select(col("userName"), col("userId"), col("position"), col("requestTime"),col("documentId"))
                            .withColumn("requestTime", col("requestTime")/60)
                            .join(formInfo, Seq("documentId"), "left")
                            .groupBy(col("formName"))
                            .agg(avg(col("requestTime")).as("averageSpeed"),count(col("formName")).as("count"))
                            .filter(col("formName").isNotNull)
                            .withColumn("type", lit(dateType))
                            .withColumn("date", lit(start_day_lit).cast("date"))
                            
        val resAvg = resUser.agg(avg(col("averageSpeed")).as("averageSpeed"), sum(col("count")).as("count"))
                    .withColumn("type", lit(dateType))
                    .withColumn("date", lit(start_day_lit).cast("date"))

        (resUser, resForm, resAvg)
  }

}
