package com.daou.analyzehome.db

import java.util.Properties
import com.mongodb.spark.MongoSpark
import org.bson.Document  
import org.apache.spark.sql.DataFrame

object DbManager {
  def writeToMongoDB(d: DataFrame, collectionName: String) = {
    MongoSpark.save(d.write.option("collection", collectionName).mode("append"))
  }

}
