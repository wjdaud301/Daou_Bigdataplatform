package com.daou.appanalyzer

import com.daou.appanalyzer.analyzer.HomeApvlAnalyze
import com.daou.appanalyzer.db.DbManager
import com.daou.appanalyzer.loader._

import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.{Level, LogManager, Logger}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */

object Analyzer {
    private val appName = "HomeApvlAnalyzer"
    val log: Logger = LogManager.getLogger(appName)
    var isDebugMode: Boolean = false

    private def printAppInfo() = {
      val version = getClass.getPackage.getImplementationVersion
      log.info(s"==============================================")
      log.info(s"    HomeApvlAnalyzer ($version)")
      log.info(s"")
      log.info(s"==============================================")
    }


    private def analyze(sc: SparkContext, fs: FileSystem) = {
      val sqlContext = new SQLContext(sc)
      val (cmprDF, targetDF) = DbManager.readMongo(sqlContext)
      val userDF = UserInfoLoader.load(sqlContext, fs)

      DbManager.writeToMongoDB(HomeApvlAnalyze.cmprLastData(sqlContext,targetDF, cmprDF, userDF), "homeApvlRank")
      DbManager.writeToMongoDB(HomeApvlAnalyze.dateRange(sqlContext),"infoDateRange")
    }



  def main(args: Array[String]): Unit = {
    log.setLevel(Level.INFO)
    printAppInfo()

    // sparkconf -> sparkSeesion
    val spark = SparkSession.builder()
                            .appName(appName)
                            .config("spark.mongodb.input.uri", "mongodb://bigdata:1q2w#E$R@pahe-common01:27017")
                            .config("spark.mongodb.output.uri", "mongodb://bigdata:1q2w#E$R@ahe-common01:27017")
                            .config("spark.mongodb.output.database", "DaouDB")
                            .getOrCreate()

    val sc = spark.sparkContext
    spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
    
    // // Analyze!
    analyze(sc, FileSystem.get(sc.hadoopConfiguration))
    sc.stop()
    


  }
}