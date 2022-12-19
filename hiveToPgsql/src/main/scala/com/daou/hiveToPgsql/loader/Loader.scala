package com.daou.hiveToPgsql.loader

import org.apache.spark.sql.{DataFrame, SparkSession}

object Loader {
  def load(sqlContext: SparkSession, dbName: String, tableName: String): DataFrame = {

    val dash_df = sqlContext.sql(s"select * from $dbName.$tableName")
    //    val dash_df = sqlContext.sql(s"select * from $dbName.$tableName")
    dash_df
  }
}