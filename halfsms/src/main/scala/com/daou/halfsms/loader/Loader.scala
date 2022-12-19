package com.daou.halfsms.loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.{DateTime}
import org.joda.time.format.DateTimeFormat

object Loader {

  def load(sqlContext: SparkSession, tableName: String, execTime: String): DataFrame = {
    val p = s"hdfs:///user/hdfs/halfsms/${tableName}/parquet_" + execTime
    sqlContext.read.parquet(p)

  }

}
