package com.daou.analyzeasset

import com.daou.analyzeasset.analyzer.AssetAnalyze
import com.daou.analyzeasset.db.DbManager
// import com.daou.analyzeasset.preprocessor.ActlogPreProcessor
import com.daou.analyzeasset.loader._

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
    private val appName = "AssetAnalyze"
    val log: Logger = LogManager.getLogger(appName)
    var isDebugMode: Boolean = false

    private def printAppInfo() = {
      val version = getClass.getPackage.getImplementationVersion
      log.info(s"==============================================")
      log.info(s"    AssetAnalyze ($version)")
      log.info(s"")
      log.info(s"==============================================")
    }


    private def analyze(sc: SparkContext, fs: FileSystem) = {
    val sqlContext = new SQLContext(sc)

    // Analyze Trlog
    val userDF = UserInfoLoader.load(sqlContext, fs)
    val assetMongoDF = DbManager.readMongo(sqlContext, userDF)

    DbManager.writeToMongoDB(AssetAnalyze.anlAssetPrefer(sqlContext, assetMongoDF), "assetPrefer")
    DbManager.writeToMongoDB(AssetAnalyze.anlDayPrefer(sqlContext, assetMongoDF), "assetDayPrefer")

    DbManager.writeToMongoDB(AssetAnalyze.anlDeptCnt(sqlContext, assetMongoDF, userDF), "assetDeptCount")
    DbManager.writeToMongoDB(AssetAnalyze.anlPosCount(sqlContext, assetMongoDF, userDF), "assetPosCount")

    DbManager.writeToMongoDB(AssetAnalyze.anlRate(sqlContext, assetMongoDF), "assetItemRate")

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
    

    // val spark = SparkSession.builder().appName("sample").enableHiveSupport().getOrCreate()//.config("hive.metastore.uris", "thrift://172.22.1.21:10002")
    // val validation = spark.sql("SELECT * FROM default.test1")
    // val vd = validation.drop("appname")
    // vd.write.mode("append").saveAsTable("default.spark_test")

  
    // spark.stop()

  }
}