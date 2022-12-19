package com.daou.appanalyzer.analyzer

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object HomeApvlAnalyze {
  def dateRange(sqlContext: SQLContext) : DataFrame = {
    import sqlContext.implicits._

    val starttime = DateTime.now().minusDays(7)
    val endtime = DateTime.now().minusDays(1)
    val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
    val startdate = starttime.toString(fmt)
    val enddate = endtime.toString(fmt)

    val df = Seq((startdate, enddate)).to .toDF("startDate", "endDate")
    val timerangeDF = df.withColumn("startDate", to_date($"startDate")).withColumn("endDate", to_date($"endDate"))

    timerangeDF
  }


  def cmprLastData(sqlContext: SQLContext, df:DataFrame, cmprDF:DataFrame, userInfo:DataFrame) : DataFrame ={
    import org.apache.spark.sql.functions.round
    import sqlContext.implicits._

    def addRanktoDF(targetdf : DataFrame) : DataFrame ={
        import org.apache.spark.sql.expressions.Window
        val w = Window.orderBy($"averageSpeed")
        val pw = Window.partitionBy($"position").orderBy($"averageSpeed")
        
        targetdf
            .distinct()
            .withColumn("totalRank", row_number().over(w))//
            .withColumn("posRank",  dense_rank().over(pw))
            .distinct()
    }

    // 초기값 세팅
//     def start_day_lit = {
//       val today = DateTime.now().minusDays(7)
//       val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
//       today.toString(fmt)
//     }

//     val targetDeptInfo = df.select($"userId", $"deptId", $"deptName", $"deptPath")
//     val posCountDF = userInfo.groupBy($"position").agg(count($"userId").as("posMemCount"))
//     val memCount = userInfo.count()

//     val res = addRanktoDF(df.drop("deptName","deptPath", "deptId", "date"))
//         .withColumn("cmprTotalRank", when($"count" =!=0 , 0))//저번주결과가 없었던건 0으로 생각하고 0에서 빼버리기!! (cmpr로 시작하는 애들 전부)
//         .withColumn("cmprPosRank", when($"count" =!= 0, 0))
//         .withColumn("cmprSpeed", when($"count" =!= 0 , 0))
//         .withColumn("averageSpeed", round($"averageSpeed" ,2))
//         .withColumn("memCount", lit(memCount))
//         .join(posCountDF, Seq("position")).distinct()
//         .withColumn("date", lit(start_day_lit).cast("date") ) //$"date".cast("date")
//         .join(targetDeptInfo, Seq("userId"),"left")
//         .distinct()


    // 주배치 코드
    val targetDeptInfo = df.select($"userId", $"deptId", $"deptName", $"deptPath")
    val posCountDF = userInfo.groupBy($"position").agg(count($"userId").as("posMemCount"))
    val memCount = userInfo.count()

    val totaluser = userInfo.withColumn("userId", $"userId".cast(IntegerType))
    val structedCmpr =  cmprDF.select($"userId", $"averageSpeed",  $"posRank", $"totalRank")
                        .join(totaluser, Seq("userId"), "outer").na.fill(0)
                        .select($"userId",$"averageSpeed",  $"posRank", $"totalRank")
                        .distinct()

    val res = addRanktoDF(df.drop("deptName","deptPath", "deptId"))
        .join(structedCmpr.select($"totalRank".alias("cmprTotalRank"),$"posRank".alias("cmprPosRank"),$"averageSpeed".alias("cmprSpeed"), $"userId"), Seq("userId"), "outer")
        .withColumn("cmprTotalRank", when($"count" =!=0 ,$"totalRank"-$"cmprTotalRank").otherwise($"totalRank")) //저번주결과가 없었던건 0으로 생각하고 0에서 빼버리기!! (cmpr로 시작하는 애들 전부)
        .withColumn("cmprPosRank", when($"count" =!= 0, $"posRank"-$"cmprPosRank").otherwise($"posRank"))
        .withColumn("cmprSpeed", when($"count" =!= 0 ,round($"averageSpeed"-$"cmprSpeed",0)).otherwise(round($"averageSpeed")))
        .withColumn("averageSpeed", round($"averageSpeed" ,2))
        .withColumn("memCount", lit(memCount))
        .join(posCountDF, Seq("position")).distinct()
        .withColumn("date", $"date".cast("date"))
        .join(targetDeptInfo, Seq("userId"),"left")
        .distinct()

    res
  }

}
