package com.daou.halfsms.db

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

object DbManager {

  def writeToHive(sc: SparkSession, df: DataFrame, dbName: String, tableName: String) {

      df.write.insertInto(dbName+"."+tableName)
  //    df.write.mode("overwrite").saveAsTable(dbName + "." + tableName)

  }
}
