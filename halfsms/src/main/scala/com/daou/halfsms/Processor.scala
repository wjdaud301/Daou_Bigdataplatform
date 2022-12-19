package com.daou.halfsms

import com.daou.halfsms.db.DbManager
import com.daou.halfsms.loader._
import com.daou.halfsms.preprocesser.{PreProcessor}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Processor {

  private val appName = "halfsms"
  val log: Logger = LogManager.getLogger(appName)
  var isDebugMode: Boolean = false

  private def printAppInfo() = {
    val version = getClass.getPackage.getImplementationVersion
    log.info(s"==============================================")
    log.info(s"    halfsms ($version)")
    log.info(s"")
    log.info(s"==============================================")
  }

  private def process(sc: SparkSession, tableName: String, execTime: String) = {

    // process DF
    val rawDF = Loader.load(sc, tableName, execTime)
    val filtered = preprocesser.PreProcessor(sc, rawDF)

    // save daou portal organization chart table
    val dbname = ""
    val tablename = tableName

    DbManager.writeToHive(sc, filtered, dbname, tablename)

  }


  def main(args: Array[String]): Unit = {
    log.setLevel(Level.INFO)
    printAppInfo()
    val tableName = args(0)
    val execTime = args(1)

    // Initialize Spark session and connect to hive
    val spark = SparkSession.builder.appName(appName).config("hive.metastore.uris", "thrift://proah-master01:9083")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport().getOrCreate()

    process(spark, tableName, execTime)
    spark.stop()

  }






}
