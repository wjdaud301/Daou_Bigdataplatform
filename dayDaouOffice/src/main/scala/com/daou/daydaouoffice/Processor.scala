package com.daou.daydaouoffice

import com.daou.daydaouoffice.db.DbManager
import com.daou.daydaouoffice.loader._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Processor {

  private val appName = "daydaouoffice"
  val log: Logger = LogManager.getLogger(appName)
  var isDebugMode: Boolean = false


  private def printAppInfo() = {
    val version = getClass.getPackage.getImplementationVersion
    log.info(s"==============================================")
    log.info(s"    domsMaster ($version)")
    log.info(s"")
    log.info(s"==============================================")
  }

  private def process(sc: SparkSession) = {

    // save daou portal organization chart table
    val dbname = "daouoffice"
    val custom_tablename = "dash_free_customer_dd"
    val user_tablename = " dash_free_user_dd"

    // process DF
    val dash_customer_df = Loader.load(sc, dbname, custom_tablename)
    val dash_user_df  = Loader.load(sc, dbname, user_tablename)

    DbManager.writeToPgSQL(sc, dash_customer_df, dbname, custom_tablename)
    DbManager.writeToPgSQL(sc, dash_user_df, dbname, user_tablename)

  }

  def main(args: Array[String]): Unit = {
    log.setLevel(Level.INFO)
    printAppInfo()


    // Initialize Spark session and connect to hive
    val spark = SparkSession.builder.appName("hiveToPgsql").config("hive.metastore.uris", "thrift://proah-master01:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("spark.sql.debug.maxToStringFields", 1000)
      .config("spark.sql.legacy.json.allowEmptyString.enabled", "True").enableHiveSupport().getOrCreate()

    process(spark)
    spark.stop()


  }

}
