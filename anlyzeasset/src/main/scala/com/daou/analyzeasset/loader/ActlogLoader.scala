package com.daou.analyzeasset.loader

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.daou.analyzeasset.Analyzer.log
import java.time.LocalDate
import java.time.format.DateTimeFormatter


object ActlogLoader {
  private def between(fromDate: LocalDate, toDate: LocalDate):String = {
        val dateList = fromDate.toEpochDay.until(toDate.toEpochDay)
        println(dateList)
        val dateMap = dateList.map(LocalDate.ofEpochDay)
        println(dateMap)
  
        val file_paths = for (dateMap <- dateMap) yield ("hdfs:///user/hdfs/daouportal/activity_log/parquet_" + dateMap.format(DateTimeFormatter.ofPattern("yyyyMMdd")))
        file_paths.mkString(",")
    }

  // sqlContext
  def load(sqlContext: SQLContext, fs: FileSystem ): DataFrame = {  
    val p = between(LocalDate.now().minusDays(7), LocalDate.now() )
    sqlContext.read.parquet(p)
      
  }
}
 