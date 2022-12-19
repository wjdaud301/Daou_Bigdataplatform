package com.daou.analyzehome.preprocesser

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

import com.daou.analyzehome.Analyzer.log


object LogPreProcessor {
        
  def makeTotalLog( sqlContext: SQLContext, userInfoDF: DataFrame,
                         weblog : DataFrame, actlogDF : DataFrame) : DataFrame ={
        import sqlContext.implicits._
        
        val deptInfoDF = userInfoDF
            .select($"userId", $"deptPath", $"deptId", $"deptName",
                     $"loginId".as("email") , $"userName", $"position")
        
        val prprcsActDF = actlogDF.filter($"companyId" === 4)
            .drop($"deptName").drop($"position")
            .join(deptInfoDF.drop($"userName"), Seq("userId"))
            .filter($"deptName" =!= "")
            .withColumn("action", 
                        when($"appName" === "EHR",
                             $"data.code").otherwise($"action")
                        )
            .withColumn("userAgent", 
                        when($"userAgent"==="GO-iPhone" || $"userAgent"==="GO-Android",
                             "Mobile").otherwise("PC")
                        )
            .select($"action", $"time",$"userAgent", $"userName", $"userId", $"deptName", $"deptId", $"deptPath",$"appName", $"position")

        val mailLog = weblog.filter($"logtype"==="webmail")
            .filter($"user" =!= "-")
            .filter($"path" === "/api/mail/message/write" ||
                     $"path" === "/api/mail/message/read" ||
                     $"path" === "/api/mail/message/send")
            .withColumn("hour", concat(
                                split((split($"time"," ").getItem(1)), ":").getItem(0)
                                )).filter($"hour" > 6)
            .withColumn("action", split($"path", "/").getItem(4))
            .withColumn("appName", lit("MAIL")).drop($"email")
            .withColumn("email", split($"user", "@").getItem(0))
            .select($"action", $"time", $"ua", $"email", $"appName")

        val worksLog = weblog.filter($"user" =!= "-")
                .filter($"method"==="POST").filter($"code" === 200)
                .filter($"path".contains("api/works/applets") || $"path".contains("api/works/activity"))
                .withColumn("email", split($"user", "@").getItem(0))
                .filter($"path".endsWith("docs") || $"path".endsWith("activity") ||$"path".endsWith("comment") )
                .withColumn("action_tmp", when($"path".contains("api/works/applets"), "regDoc").otherwise("comment"))
                .withColumn("action", when($"path".endsWith("activity"), "regActRecord").otherwise($"action_tmp"))
                .withColumn("appName", lit("WORKS"))
                .select($"action", $"time", $"ua", $"email", $"appName")

        val unionWebLog = mailLog.union(worksLog)
                            .withColumn("userAgent", 
                                when($"ua.os.family"==="iOS" || $"ua.os.family"==="Android", 
                                    "Mobile").otherwise("PC")
                            )
                            .join(deptInfoDF, Seq("email"))
                            .select($"action", $"time",$"userAgent", $"userName",
                                    $"userId", $"deptName", $"deptId", $"deptPath",
                                    $"appName", $"position")              

        val res = prprcsActDF.union(unionWebLog)
            .withColumn("date", $"time".cast("date"))
            .withColumn("hour", concat(
                                split((split($"time"," ").getItem(1)), ":").getItem(0)
                                )).filter($"hour" > 5)
               
        res
    }
}
