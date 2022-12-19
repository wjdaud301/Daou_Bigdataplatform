package com.daou.analyzehome

import com.daou.analyzehome.analyzer.HomeAnalyze
import com.daou.analyzehome.db.DbManager
import com.daou.analyzehome.preprocesser.LogPreProcessor
import com.daou.analyzehome.loader._

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, LogManager, Logger}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */

object Analyzer {
    private val appName = "AnalyzeHome"
    val log: Logger = LogManager.getLogger(appName)
    var isDebugMode: Boolean = false

    private def printAppInfo() = {
      val version = getClass.getPackage.getImplementationVersion
      log.info(s"==============================================")
      log.info(s"    AnalyzeHome ($version)")
      log.info(s"")
      log.info(s"==============================================")
    }

    private def analyze(sc: SparkContext, fs: FileSystem) = {
      val sqlContext = new SQLContext(sc)

      // Analyze Trlog
      val rawActLog = ActlogLoader.load(sqlContext, fs)
      val userDF = UserInfoLoader.load(sqlContext, fs)
      val rawWebLog = WeblogLoader.load(sqlContext, fs)

      val filtered = LogPreProcessor.makeTotalLog(sqlContext, userDF, rawWebLog, rawActLog)

      DbManager.writeToMongoDB(HomeAnalyze.userAction(sqlContext, filtered), "homeUsrActPtrn")
      DbManager.writeToMongoDB(HomeAnalyze.makeEhrRank(sqlContext, filtered), "homeClockRank")
      DbManager.writeToMongoDB(HomeAnalyze.userUsageRate(sqlContext, userDF, filtered), "homeUserUsage")
      DbManager.writeToMongoDB(HomeAnalyze.deptUsageRate(sqlContext, userDF, filtered), "homeDeptUsage")
    }


    def main(args: Array[String]): Unit = {
      log.setLevel(Level.INFO)
      printAppInfo()

      // sparkconf -> sparkSeesion
      val spark = SparkSession.builder()
                              .appName(appName)
                              .config("spark.mongodb.input.uri", "mongodb://bigdata:1q2w#E$R@proah-common01:27017")
                              .config("spark.mongodb.output.uri", "mongodb://bigdata:1q2w#E$R@proah-common01:27017")
                              .config("spark.mongodb.output.database", "DaouDB")
                              .getOrCreate()


      // val sc = new SparkContext(sparkConf) 
      val sc = spark.sparkContext
      spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
      
      // // Analyze!
      analyze(sc, FileSystem.get(sc.hadoopConfiguration))
      sc.stop()

      // spark.stop()

    }
}