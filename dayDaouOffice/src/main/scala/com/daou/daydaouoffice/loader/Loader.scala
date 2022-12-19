package com.daou.daydaouoffice.loader

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object Loader {
  def load(sqlContext: SparkSession, dbName: String, tableName: String): DataFrame = {

    val dash_df = sqlContext.sql(s"select * from $dbName.$tableName where yyyymmdd = date_sub(current_date(), 1)")
//    val dash_df = sqlContext.sql(s"select * from $dbName.$tableName")
    dash_df
  }
}
