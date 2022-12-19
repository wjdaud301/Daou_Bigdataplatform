package com.daou.hiveToPgsql

import com.daou.hiveToPgsql.db.DbManager
import com.daou.hiveToPgsql.loader._
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession


object Processor {

  private val appName = "hiveToPgsql"
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
    val tablename = "dash_access_monitoring_dd"

    // process DF
    val log_df = Loader.load(sc, dbname, tablename)

    DbManager.writeToPgSQL(sc, log_df, dbname, tablename)

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
