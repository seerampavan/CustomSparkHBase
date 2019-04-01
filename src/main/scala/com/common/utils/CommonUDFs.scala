package com.common.utils

import org.apache.spark.sql.functions._
import com.common.utils.db.HBaseDBUtil
object CommonUDFs {

  def getHBaseData = udf[String, String, String, String, String]((tableName: String, rowkey:String, columnFamily:String, columnQualifier: String) => {

    var respValue :String = HBaseDBUtil.getDataFromHbase(tableName:String, rowkey:String, columnFamily:String, columnQualifier: String)
    if(respValue == null || respValue.isEmpty) {
      null
    }
    respValue
  })

}
