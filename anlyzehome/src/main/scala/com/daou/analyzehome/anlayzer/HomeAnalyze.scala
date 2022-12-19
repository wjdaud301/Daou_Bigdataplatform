package com.daou.analyzehome.analyzer

import org.joda.time.{DateTime}
import org.joda.time.format.DateTimeFormat

import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions._
import com.daou.analyzehome.Analyzer.log

object HomeAnalyze {

      def start_day_lit = {
            val today = DateTime.now().minusDays(7)
            val fmt = DateTimeFormat.forPattern("yyyy-MM-dd")
            today.toString(fmt)
      }
      
      private val workingDays = 7.0
      private val dateType = "week"

      /*출근해서 n번째로 하는 일*/
      def userAction(sqlContext: SQLContext,df : DataFrame):DataFrame={
            import sqlContext.implicits._
            import org.apache.spark.sql.functions._

            val agentdf = df.groupBy($"userId", $"action", $"userAgent").count()
            .select(struct($"count", $"userAgent").alias("agc"), 
                        $"userId", $"action")
            .groupBy($"userId", $"action")
                  .agg(max($"agc").as("agc"))
            .select($"userId", $"action", $"agc.userAgent")
            
            val dailyFirstAction =  df.withColumn("time", to_timestamp($"time","yyyy-MM-dd HH:mm:ss X"))
            .select(struct($"time",$"appName",  $"action").alias("vs"),
                        $"date", $"userName",$"userId", col("position"),
                        $"deptName",$"deptId",$"deptPath")
            .groupBy($"date", $"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath")
                  .agg(min("vs").alias("vs"))
            .select($"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath", $"date", $"vs.time",$"vs.appName",$"vs.action")
            .groupBy($"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath", $"appName", $"action").count()
            
            val userFirstAction = dailyFirstAction.select(struct( $"count", $"appName", $"action").alias("st"),$"userName", $"userId", col("position"), $"deptName", $"deptId", $"deptPath")
            .groupBy($"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath")
                  .agg(max("st").alias("st"))
            .select($"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath", $"st.appName", $"st.action")
                  
            val tmp = df.select($"userName", $"userId" , col("position"),$"deptName",$"deptId",$"deptPath", $"appName", $"action")
            
            val userSecondAction = df.join(userFirstAction,(userFirstAction("userId") === tmp("userId") && userFirstAction("action") ===tmp("action")), "leftanti")
            .select(struct($"appName",  $"action").alias("vs"),
                        $"date", $"userName",$"userId", col("position"),
                        $"deptName",$"deptId",$"deptPath")     
            .groupBy($"date", $"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath").agg(min("vs").alias("vs"))
            .select($"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath", $"date", $"vs.appName",$"vs.action")
            .groupBy($"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath", $"appName", $"action")
                  .count().orderBy($"count". desc)// 여기까지는 유저별 사용한 첫 액션들이 나옴(어떤날은 채팅을 가장먼저, 어떤날은 근태를 가장먼저)
            .select(struct($"count", $"appName", $"action").alias("st"),
                        $"userName", $"userId", col("position"),
                        $"deptName", $"deptId", $"deptPath")
            .groupBy($"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath")
                  .agg(max("st").alias("st"))
            .select($"userName",$"userId", col("position"),$"deptName",$"deptId",$"deptPath", $"st.appName", $"st.action")
            
            val unionDF = userFirstAction.withColumn("code", lit("firstAction")).union(userSecondAction.withColumn("code", lit("secondAction")))
            
            unionDF.join(agentdf,(unionDF("userId") === agentdf("userId") && unionDF("action") === agentdf("action")))
                  .withColumn("type", lit(dateType))
                  .withColumn("date", lit(start_day_lit).cast("date"))

      }

      /*근태 랭킹*/
      def makeEhrRank(sqlContext: SQLContext,df : DataFrame):DataFrame = {
            import sqlContext.implicits._
            import org.apache.spark.sql.types._
            import org.apache.spark.sql.functions._

            val tsCol = col("time").cast(TimestampType)
            
            val clockInDF = df.filter(col("action") === "defaultClockIn")
            .withColumn("time", to_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss X"))
            .filter(hour(tsCol).between(6, 10))
            .withColumn("tmp", lit("2020-01-01"))
            .withColumn("time", (split(col("time")," ").getItem(1)))
            .withColumn("time",
                        concat(col("tmp"), lit(" "), (col("time")) ).cast("timestamp")
                        )
            .groupBy(col("userId"),col("userName"), col("deptName"), col("deptId"), col("deptPath"), col("position"))
                  .agg(avg(unix_timestamp(col("time"))).as("average"))
            .select(col("userId"),col("userName"), col("position"), col("deptName"), col("deptId"), col("deptPath"), col("average").cast("timestamp"))
            .withColumn("averageTime", 
                        split(split(col("average")," ").getItem(1), "\\.").getItem(0))
                        
            val clockOutDF = df.filter(col("action") === "defaultClockOut")
            .withColumn("time", to_timestamp(col("time"),"yyyy-MM-dd HH:mm:ss X"))
            .filter(hour(tsCol).between(15, 22))
            .withColumn("tmp", lit("2020-01-01"))
            .withColumn("time", (split(col("time")," ").getItem(1)))
            .withColumn("time",
                        concat(col("tmp"), lit(" "), (col("time")) ).cast("timestamp"))
            .groupBy(col("userId"),col("userName"), col("deptName"), col("deptId"), col("deptPath"), col("position"))
                  .agg(avg(unix_timestamp(col("time"))).as("average"))
            .select(col("userId"),col("userName"), col("position"), col("deptName"), col("deptId"), col("deptPath"), col("average").cast("timestamp"))
            .withColumn("averageTime", 
                        split(split(col("average")," ").getItem(1), "\\.").getItem(0)
                        )
            
            import org.apache.spark.sql.expressions.Window
            val cw = Window.orderBy(col("average"))
            val clockOutRank = clockOutDF
                  .withColumn("rank", row_number().over(cw)).drop(col("average"))
                  .withColumn("code",lit("defaultClockOut"))
            val clockInRank = clockInDF
                  .withColumn("rank", row_number().over(cw)).drop(col("average"))
                  .withColumn("code",lit("defaultClockIn"))
            
            clockOutRank.union(clockInRank).withColumn("type", lit(dateType))
                        .withColumn("date", lit(start_day_lit).cast("date"))
      }

      def userUsageRate(sqlContext: SQLContext,userInfoDF: DataFrame, df: DataFrame):DataFrame = {
            import sqlContext.implicits._
            import org.apache.spark.sql.expressions.Window
            import org.apache.spark.sql.types._
            
            val totalcnt = userInfoDF.select(col("userId")).distinct().count()

            //여기 df는 사전에 미리 dept정보가 조인이 되어있는 친구여야함.
            val res = df.select(col("appName"), col("userName"),col("userId"),
                              col("deptName"),col("deptId"),col("deptPath"), col("date"))
                        .distinct()
                        .groupBy(col("userName"),col("userId"), col("deptName"),
                                    col("deptId"),col("deptPath"), col("appName")).count()
                        .withColumn("usagePercent", col("count")/workingDays * 100 )
                        .groupBy(col("userName"),col("userId"),col("deptName"),col("deptId"),col("deptPath"))
                        .pivot(col("appName")).agg(avg(col("usagePercent"))).na.fill(0) // <- pivot만드는거
                        .withColumn("avgRate", 
                        round((col("APPROVAl")+col("ASSET")+col("WORKS")+col("BOARD")+col("CALENDAR")+col("MAIL")+col("CHAT")+col("EHR")+col("REPORT"))/9, 2))
                        .select(col("userName"), col("userId"),col("deptName"),
                              col("deptId"), col("deptPath"), col("avgRate"))
                        .withColumn("rank", rank().over(Window.orderBy(col("avgRate").desc)))
                        .withColumn("memCount", lit(totalcnt))
                        .withColumn("type", lit(dateType))
                        .withColumn("date", lit(start_day_lit).cast("date"))
                        .withColumn("userId", col("userId").cast(IntegerType))

            res
      }

      /*부서별 사용량 - 앱별 , 평균, 등수(최하위레벨인 경우만) */
      def deptUsageRate(sqlContext: SQLContext,deptInfoDF: DataFrame, df : DataFrame):DataFrame={
            import sqlContext.implicits._
            import org.apache.spark.sql.expressions.Window

            // 상하위 부서 나눠서 계산되도록
            val idNameSet= deptInfoDF.select($"deptId", $"deptName").distinct()
            val userUpDownDept = deptInfoDF.select($"userId", explode(split($"deptPath", ":")).as("deptId"))
                                    .union(deptInfoDF.select($"userId", $"deptId"))
                                    .join(idNameSet, Seq("deptId"))
            val p_list = deptInfoDF.select("deptPath").distinct()
                        .map(r => r.getString(0)).collect.toList.flatMap(_.split(":"))
            val d_list = deptInfoDF.select("deptId").distinct()
                        .map(r => r.getString(0)).collect.toList 
            val lowest_dept_list= d_list diff p_list // 최하위 레벨에 있는 팀들 뽑는 거

            val deptMemCount = userUpDownDept.groupBy($"deptId").count()
            .withColumnRenamed("count", "memCount")
            
            val caluclatedDF = df.drop("deptId", "deptName", "deptPath")
            .join(userUpDownDept, Seq("userId"))
            .select($"appName",$"userId", $"deptName",$"deptId", $"date").distinct()
            .groupBy($"deptName",$"deptId", $"appName").count()
            .join(deptMemCount, Seq("deptId"))
            .filter($"memCount" =!= 1)
            .withColumn("perCount", $"count"/$"memCount")
            .groupBy($"deptName",$"deptId", $"appName")
                  .agg(avg($"perCount").as("avgPerCount"))
            .withColumn("usagePercent", round($"avgPerCount"/workingDays * 100 ,2))
            .groupBy($"deptName",$"deptId")
                  .pivot($"appName")
                  .agg(avg($"usagePercent")).na.fill(0) // <- pivot만드는거
            .withColumn("avgRate", ($"APPROVAl"+$"ASSET"+$"WORKS"+$"BOARD"+$"CALENDAR"+$"MAIL"+$"CHAT"+$"EHR"+$"REPORT")/9)
            
            val res = caluclatedDF.withColumn("flag", when($"deptId".isin(lowest_dept_list: _*), 0).otherwise(1) ) // 0 : 최하위 레벨
                        .withColumn("rank", when($"flag"===0 , rank().over(Window.orderBy($"avgRate".desc))).otherwise(0) )
                        .withColumn("type", lit(dateType))
                        .withColumn("date", lit(start_day_lit).cast("date"))
            
            res
      }
}
