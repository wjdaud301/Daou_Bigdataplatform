package com.daou.analyzeboard.preprocessor

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.daou.analyzeboard.Analyzer.log

object ActlogPreProcessor {
  private def start_day_lit = {
    val today = DateTime.now().minusDays(7)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    today.toString(fmt)
  }

  private val day_type = "week"
  
  def preprocess(sqlContext: SQLContext, rawActLog: DataFrame, userInfo: DataFrame): DataFrame = {
    import sqlContext.implicits._

    // Select some fields and filter useless rows
    val res = rawActLog.filter($"companyId" === 4) // 다우기술만
            .filter($"appName" === "BOARD")
            .select(
                $"userId",$"time", $"action",
                $"data.postId".alias("postId")
            ).withColumn("time", to_timestamp($"time","yyyy-MM-dd HH:mm:ss X"))
            .join(userInfo, Seq("userId"), "left")
            .filter($"deptName".isNotNull).filter($"deptName" =!= "null")
            .repartition($"deptName")
            .withColumn("type", lit(day_type))
            .withColumn("date", lit(start_day_lit).cast("date"))

   res
  }
}