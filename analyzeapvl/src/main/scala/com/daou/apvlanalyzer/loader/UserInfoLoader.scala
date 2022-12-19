package com.daou.apvlanalyzer.loader

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.daou.apvlanalyzer.Analyzer.log
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object UserInfoLoader {
  private def getToday = {
    val today = DateTime.now()
    val fmt = DateTimeFormat.forPattern("yyyyMMdd")
    today.toString(fmt)
  }

  def load(sqlContext: SQLContext, fs: FileSystem): DataFrame = {
    val p ="hdfs:///user/hdfs/daouportal/org_db/parquet_"+ getToday
    sqlContext.read.parquet(p)
  }
}