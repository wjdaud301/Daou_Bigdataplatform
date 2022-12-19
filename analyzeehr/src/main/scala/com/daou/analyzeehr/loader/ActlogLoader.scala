package com.daou.analyzeehr.loader

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SQLContext}
import com.daou.analyzeehr.Analyzer.log
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import scala.collection.mutable.MutableList


object ActlogLoader {
  private def between(fromDate: LocalDate, toDate: LocalDate):MutableList[String] = {
        val dateList = fromDate.toEpochDay.until(toDate.toEpochDay)
        println(dateList)
        val dateMap = dateList.map(LocalDate.ofEpochDay)
        println(dateMap)
  
        var file_paths: MutableList[String] = MutableList()
        for(dateMap <- dateMap){
          file_paths = file_paths :+ "hdfs:///user/hdfs/daouportal/activity_log/parquet_" + dateMap.format(DateTimeFormatter.ofPattern("yyyyMMdd"))
        }

        file_paths
    }
    

  // sqlContext
  def load(sqlContext: SQLContext, fs: FileSystem ): DataFrame = {  
    val p = between(LocalDate.now().minusDays(7), LocalDate.now() )
    sqlContext.read.parquet(p: _*)
      
  }
}
