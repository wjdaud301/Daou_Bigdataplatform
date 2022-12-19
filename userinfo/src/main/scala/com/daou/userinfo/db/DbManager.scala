package com.daou.userinfo.db

import java.util.Properties
import com.mongodb.spark.MongoSpark
import org.bson.Document  
import org.apache.spark.sql.DataFrame


object DbManager {  

  def writeToMongoDB(d: DataFrame) {
    MongoSpark.save(d.write.option("collection", "infoUserDept").mode("append"))
  } 

}
