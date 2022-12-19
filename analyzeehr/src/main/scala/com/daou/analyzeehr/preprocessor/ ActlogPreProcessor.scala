package com.daou.analyzeehr.preprocessor

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.daou.analyzeehr.Analyzer.log
import com.daou.analyzeehr.analyzer.EhrAnalyze.start_day_lit

object ActlogPreProcessor {

  def preprocess(sqlContext: SQLContext, rawActLog: DataFrame, userInfo: DataFrame): DataFrame = {
    import sqlContext.implicits._

    // Select some fields and filter useless rows
    val res = rawActLog.filter($"companyId" === 4) // 다우기술만
            .filter($"appName" === "EHR")
            .select(
                $"userId", $"position",$"time", //,$"deptId", $"deptName"
                $"data.code".alias("code")
            ).withColumn("day", date_format(to_date($"time", "yyyy-MM-dd HH:mm:ss X"),"EEEE"))
            .withColumn("date", $"time".cast("date"))
            .join(userInfo.drop("time"), Seq("userId"), "left")
            .filter($"code" === "defaultClockIn" || $"code" === "defaultClockOut")
            .filter($"deptName".isNotNull).filter($"deptName" =!= "null")

    res
  }
}
