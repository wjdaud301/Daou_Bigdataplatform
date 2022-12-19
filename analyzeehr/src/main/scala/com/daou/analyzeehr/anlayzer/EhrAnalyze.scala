package com.daou.analyzeehr.analyzer

import org.joda.time.{DateTime}
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import com.daou.analyzeehr.Analyzer.log


object EhrAnalyze {

  private def start_day_lit = {
    val today = DateTime.now().minusDays(7)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    today.toString(fmt)
  }


  private val day_type = "week"

   def anlDeptAvg(sqlContext: SQLContext,targetDF : DataFrame):DataFrame ={
         val res = targetDF
               .withColumn("time", to_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss X"))
               .withColumn("tmp", lit("2020-01-01"))
               .withColumn("time", (split(col("time")," ").getItem(1)))
               .withColumn("time",
                        concat(col("tmp"), lit(" "), (col("time")) ).cast("timestamp")
                           )
               .groupBy( col("deptName"),col("deptId"), col("code"))
               .agg(avg(unix_timestamp(col("time"))).as("average"))
               .select(col("deptName"), col("deptId"), col("code"), col("average").cast("timestamp"))
               .withColumn("average", 
                           split(split(col("average")," ").getItem(1), "\\.").getItem(0))
               .withColumn("type", lit(day_type))
               .withColumn("date", lit(start_day_lit).cast("date"))

         res
   }
      
   def anlTotAvgt(sqlContext: SQLContext, targetDF: DataFrame):DataFrame ={
      import sqlContext.implicits._
      val res = targetDF.groupBy(col("code"), window(col("time"), "30 minutes")).count()
               .select($"code", $"window.*", $"count").withColumn("start", split($"start", " ").getItem(1))
               .groupBy($"code", $"start").agg(avg($"count") as "count")
               .withColumnRenamed("start", "time")
               .withColumn("type", lit(day_type))
               .withColumn("date", lit(start_day_lit).cast("date"))
      
      res
   }   

   def anlDayDeptAvg(sqlContext: SQLContext,targetDF : DataFrame):DataFrame ={
         val res = targetDF
                     .withColumn("time", to_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss X"))
               .withColumn("tmp", lit("2020-01-01"))
               .withColumn("time", (split(col("time")," ").getItem(1)))
               .withColumn("time",
                        concat(col("tmp"), lit(" "), (col("time")) ).cast("timestamp")
                           )
               .groupBy( col("deptName"),col("deptId"), col("code"),col("day"))
                  .agg(avg(unix_timestamp(col("time"))).as("average"), count("userId").as("count"))
               .select(col("deptName"), col("deptId"), col("code"),col("day"),col("count"), col("average").cast("timestamp"))
               .withColumn("average", 
                           split(split(col("average")," ").getItem(1), "\\.").getItem(0))
               .withColumn("type", lit(day_type))
               .withColumn("date", lit(start_day_lit).cast("date"))
            
         res
   }
   
}
