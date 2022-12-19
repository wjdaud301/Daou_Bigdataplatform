package com.daou.userinfo.loader

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.daou.userinfo.Analyzer.log
import org.apache.spark.sql.functions.{lit, to_date}

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


object UserInfoLoader {
  private def getYesterday = {
    val yesterday = DateTime.now().minusDays(1)
    val fmt = DateTimeFormat.forPattern("yyyyMMdd")
    yesterday.toString(fmt)
  }

  private def getToday = {
    val today = DateTime.now()
    val fmt = DateTimeFormat.forPattern("y-M-d")
    today.toString(fmt)
  }


  def load(sqlContext: SQLContext, fs: FileSystem): DataFrame = {
    import sqlContext.implicits._
    val p ="hdfs:///user/hdfs/daouportal/org_db/parquet_"+ getYesterday
    val rawDF = sqlContext.read.parquet(p)

    val res = rawDF.withColumn("date", to_date(lit(getToday))).drop($"time")

    res
  }
}
