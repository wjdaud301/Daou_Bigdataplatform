package com.daou.apvlanalyzer

import com.daou.apvlanalyzer.analyzer.ApvlAnalyze
import com.daou.apvlanalyzer.db.DbManager
import com.daou.apvlanalyzer.preprocesser.ActlogPreProcessor
import com.daou.apvlanalyzer.loader._

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
    private val appName = "ApvlAnalyzer"
    val log: Logger = LogManager.getLogger(appName)
    var isDebugMode: Boolean = false

    private def printAppInfo() = {
      val version = getClass.getPackage.getImplementationVersion
      log.info(s"==============================================")
      log.info(s"    ApvlAnalyzer ($version)")
      log.info(s"")
      log.info(s"==============================================")
    }


    private def analyze(sc: SparkContext, fs: FileSystem) = {
        val sqlContext = new SQLContext(sc)
    
        // Analyze Trlog
        val rawlogDF = ActlogLoader.load(sqlContext, fs)
        val userDF = UserInfoLoader.load(sqlContext, fs)
        val (apvlDF, draftDF) = ActlogPreProcessor.preprocess(sqlContext, rawlogDF, userDF)

        DbManager.writeToMongoDB(ApvlAnalyze.anlUseDay(sqlContext, draftDF), "apvlUsePerDay")
        DbManager.writeToMongoDB(ApvlAnalyze.anlFormUseDay(sqlContext, draftDF), "apvlFormPerDay")

        DbManager.writeToMongoDB(ApvlAnalyze.anlUserCnt(sqlContext, draftDF), "apvlDraftUserCount")
        DbManager.writeToMongoDB(ApvlAnalyze.anlPosCnt(sqlContext, draftDF, userDF), "apvlDraftPosCount")
        DbManager.writeToMongoDB(ApvlAnalyze.anlDeptCnt(sqlContext, draftDF, userDF), "apvlDraftDeptCount")

        DbManager.writeToMongoDB(ApvlAnalyze.draftCmprWdr(sqlContext, apvlDF), "apvlCmplWdraw")

        /* approval speed */
        val (speedUserDF, speedFormDF, speedAvgDF) = ApvlAnalyze.analyzeSpeed(sqlContext, draftDF, apvlDF)
        DbManager.writeToMongoDB(speedUserDF, "apvlSpeed")
        DbManager.writeToMongoDB(speedFormDF, "apvlSpeedForm")
        DbManager.writeToMongoDB(speedAvgDF, "apvlSpeedAvg")

    
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


    val sc = spark.sparkContext
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    // // Analyze!
    analyze(sc, FileSystem.get(sc.hadoopConfiguration))
    sc.stop()

  
    // spark.stop()

  }
}