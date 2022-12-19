package com.daou.apvlanalyzer.preprocesser

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.daou.apvlanalyzer.Analyzer.log
import com.daou.apvlanalyzer.analyzer.ApvlAnalyze.start_day_lit

object ActlogPreProcessor {

  def preprocess(sqlContext: SQLContext, rawActLog: DataFrame, userInfo: DataFrame): (DataFrame, DataFrame) = {
    import sqlContext.implicits._

    val approveDF = rawActLog.filter($"companyId" === 4) // 다우기술만
            .filter($"appName" === "APPROVAl")
            .filter($"action" === "APPROVAL")
            .select(
                 $"userId",$"time",
                $"data.documentId".alias("documentId"),
                $"data.prevCompletedAt".alias("prevCompletedAt"),
                $"data.docStatus".alias("docStatus"),
                $"data.apprStatus".alias("apprStatus")
            )
        //     .withColumn("day", to_timestamp($"time","yyyy-MM-dd HH:mm:ss X"))
            .withColumn("day", date_format(to_date($"time", "yyyy-MM-dd HH:mm:ss X"),"EEEE"))
            .withColumn("targetDate", $"time".cast("date"))
            .join(userInfo.drop("time"), Seq("userId"), "left")
            .withColumn("prevCompletedAt", concat( 
                                                (split($"prevCompletedAt","T").getItem(0)), 
                                                lit(" "), 
                                                (split($"prevCompletedAt","T").getItem(1))  
                                            ))
            .withColumn("type", lit("week"))
            .withColumn("date", lit(start_day_lit).cast("date"))


    val draftDF = rawActLog.filter($"companyId" === 4) // 다우기술만
            .filter($"appName" === "APPROVAl")
            .filter($"action" === "DRAFT")
            .select(
                    $"userId", $"time",
                    $"data.formName".alias("formName"),
                    $"data.documentId".alias("documentId"),
                    $"data.drafterName".alias("drafterName")
            )
        //     .withColumn("day", to_timestamp($"time","yyyy-MM-dd HH:mm:ss X"))
            .withColumn("day", date_format(to_date($"time","yyyy-MM-dd HH:mm:ss X"),"EEEE"))
            .withColumnRenamed("time", "tmp_time")
            .join(userInfo, Seq("userId"), "left")
            .withColumn("type", lit("week"))
            .withColumn("date", lit(start_day_lit).cast("date"))

    (approveDF, draftDF)
  }
}
