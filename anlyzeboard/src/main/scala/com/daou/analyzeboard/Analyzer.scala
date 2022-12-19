package com.daou.analyzeboard

import com.daou.analyzeboard.analyzer.BoardAnalyze
import com.daou.analyzeboard.db.DbManager
import com.daou.analyzeboard.preprocessor.ActlogPreProcessor
import com.daou.analyzeboard.loader._

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
    private val appName = "AnalyzeBoard"
    val log: Logger = LogManager.getLogger(appName)
    var isDebugMode: Boolean = false

    private def printAppInfo() = {
      val version = getClass.getPackage.getImplementationVersion
      log.info(s"==============================================")
      log.info(s"    AnalyzeBoard ($version)")
      log.info(s"")
      log.info(s"==============================================")
    }


    private def analyze(sc: SparkContext, fs: FileSystem) = {
      val sqlContext = new SQLContext(sc)

      // Analyze Trlog
      val rawlogDF = ActlogLoader.load(sqlContext, fs)
      val userDF = UserInfoLoader.load(sqlContext, fs)

      val filtered = ActlogPreProcessor.preprocess(sqlContext, rawlogDF, userDF)

      val postDF = DbManager.readMongo(sqlContext)

      DbManager.writeToMongoDB(BoardAnalyze.anlDeptCnt(sqlContext, filtered, userDF,"RECOMMEND"), "boardRcmdDeptCount")
      DbManager.writeToMongoDB(BoardAnalyze.anlDeptCnt(sqlContext, filtered, userDF,"READ_CONTENT"), "boardReadDeptCount")
      DbManager.writeToMongoDB(BoardAnalyze.anlDeptCnt(sqlContext, filtered, userDF,"CREATE_CONTENT"), "boardWriteDeptCount")

      DbManager.writeToMongoDB(BoardAnalyze.anlPosCnt(sqlContext, filtered, userDF,"RECOMMEND"), "boardRcmdPosCount")
      DbManager.writeToMongoDB(BoardAnalyze.anlPosCnt(sqlContext, filtered, userDF,"READ_CONTENT"), "boardReadPosCount")
      DbManager.writeToMongoDB(BoardAnalyze.anlPosCnt(sqlContext, filtered, userDF,"CREATE_CONTENT"), "boardWritePosCount")
      
      DbManager.writeToMongoDB(BoardAnalyze.anlUserCnt(sqlContext, filtered, "RECOMMEND"), "boardRcmdUserCount")
      DbManager.writeToMongoDB(BoardAnalyze.anlUserCnt(sqlContext, filtered, "READ_CONTENT"), "boardReadUserCount")
      DbManager.writeToMongoDB(BoardAnalyze.anlUserCnt(sqlContext, filtered, "CREATE_CONTENT"), "boardWriteUserCount")

      DbManager.writeToMongoDB(BoardAnalyze.anlPostCnt(sqlContext, filtered, postDF, "CREATE_CONTENT"), "boardRcmdPostCount")
      DbManager.writeToMongoDB(BoardAnalyze.anlPostCnt(sqlContext, filtered, postDF, "READ_CONTENT"), "boardReadPostCount")
    
    }



    def main(args: Array[String]): Unit = {
      log.setLevel(Level.INFO)
      printAppInfo()

      // sparkconf -> sparkSeesion
      val spark = SparkSession.builder()
                              .appName(appName)
                              .config("spark.mongodb.input.uri", "mongodb://bigdata:1q2w#E$R@ahe-common01:27017")
                              .config("spark.mongodb.output.uri", "mongodb://bigdata:1q2w#E$R@ahe-common01:27017")
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