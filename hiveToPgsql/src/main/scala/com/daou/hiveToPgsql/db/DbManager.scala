package com.daou.hiveToPgsql.db

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties

object DbManager {

  def writeToPgSQL(sc: SparkSession, df: DataFrame, dbName: String, tableName: String): Unit = {
    val host = "proah-common01" // ip
    val port = "5432" //port
    val user = "postgres" //user name
    val pw = "postgres" //password

    val prop = new Properties()
    prop.setProperty("user", user)
    prop.setProperty("password", pw)

    df.write.option("driver", "org.postgresql.Driver").mode("overwrite").jdbc(s"jdbc:postgresql://$host:$port/$dbName", tableName, prop)
  }
}