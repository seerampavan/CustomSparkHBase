package com.common.utils

import com.common.utils.CommonUtilities._
import com.common.utils.Configuration._
import com.common.utils.db.HBaseDBUtil
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * This loads data from Hive or file and save data to
  */
object PullHive_HBaseData {

  def main(args: Array[String]) {

    //clusterORLocal=l hiveORFile=file sqlQueryOrCSVPath=data/mllib/adult.csv tableNameIfAny=temptable  sparkOrHiveContext=hive hiveTableName= partitionColumns= isOverwriteDataOk=y dataLocationToSave=tmp hbaseConfigXML= populateColValFromHBase= udf_hbaseTableName=t1 udf_hbaseRowkey=1 udf_hbaseColumnFamily=c udf_hbaseColLookup=native_country saveDataToHbase=y save_hbaseTableName=testhbasetable save_hbaseRowkey=1 save_hbaseColumnFamily=c save_numOfSalts=6
    val namedArgs = getNamedArgs(args)

    if(namedArgs.size ==0) throw new Exception(" Unable to read the arguments "+args)

    val runLocal = namedArgs("clusterORLocal")
    val isDataFromHive = namedArgs("hiveORFile")
    val sqlQueryOrCSVPath = namedArgs("sqlQueryOrCSVPath")
    val sparkOrHiveContext:String = namedArgs("sparkOrHiveContext")

    val locationToSaveData = namedArgs("locationToSaveData")
    val hiveTableNameToSave:String = namedArgs("hiveTableNameToSave")
    val partitionColumns:String = namedArgs("partitionColumns")
    val isOverwriteDataOk = namedArgs("isOverwriteDataOk").equalsIgnoreCase("y")
    val isTempTableNeeded = namedArgs("isTempTableNeeded").equalsIgnoreCase("y")

    val hbaseConfigXML:String = namedArgs("hbaseConfigXML")
    val udf_hbaseTableName = namedArgs("udf_hbaseTableName")
    val udf_hbaseRowkey = namedArgs("udf_hbaseRowkey")
    val udf_hbaseColumnFamily = namedArgs("udf_hbaseColumnFamily")
    val udf_hbaseColLookup = namedArgs("udf_hbaseColLookup")

    val saveDataToHbase = namedArgs("saveDataToHbase").equalsIgnoreCase("y")
    val save_hbaseTableName = namedArgs("save_hbaseTableName")
    val save_hbaseRowkey = namedArgs("save_hbaseRowkey")
    val save_hbaseColumnFamily = namedArgs("save_hbaseColumnFamily")
    val save_numOfSalts = namedArgs("save_numOfSalts").toInt

    val applicationName = "Perform Computation save to HIVE an HBASE"
    val sc: SparkContext = getSparkContext(runLocal, applicationName)

    val sqlContext :SQLContext = if ("hive".equalsIgnoreCase(sparkOrHiveContext)) new HiveContext(sc) else new SQLContext(sc)

    //get the dataframe from the source file
    val dataframe = getDataframeFromHiveORFile(sqlContext, isDataFromHive, sqlQueryOrCSVPath)

    val config = if(hbaseConfigXML != null && !hbaseConfigXML.isEmpty) {
      HBaseDBUtil.getHBaseDBConfiguration(hbaseConfigXML)
    } else null

    //populate new column in dataframe by the data from HBase
    val dataframeToSave = if(config != null && notEmpty(udf_hbaseColLookup) && notEmpty(udf_hbaseTableName) && notEmpty(udf_hbaseRowkey) && notEmpty(udf_hbaseColumnFamily)) {
      populateColumnFromHBase(dataframe: DataFrame, udf_hbaseColLookup: String, udf_hbaseTableName: String, udf_hbaseRowkey: String, udf_hbaseColumnFamily: String)
    }else{
      dataframe
    }

    if(notEmpty(locationToSaveData)){
      //Saving the dataframe to HDFS
      val saveDFWithMode = dataframeToSave.write.mode(if(isOverwriteDataOk) SaveMode.Overwrite else SaveMode.Append)

      if(partitionColumns != null && !partitionColumns.isEmpty)
        saveDFWithMode.partitionBy(partitionColumns.split(","):_*).save(locationToSaveData)
      else
        saveDFWithMode.save(locationToSaveData)

      if(notEmpty(hiveTableNameToSave)){
        createHiveExternalTableWithNoPartitions(sqlContext, locationToSaveData, "parquet", hiveTableNameToSave, isOverwriteDataOk)
      }
    }else if(hiveTableNameToSave != null && !hiveTableNameToSave.isEmpty){

      val tempHiveTable = if(hiveTableNameToSave.split("\\.").length > 1) "temp_"+hiveTableNameToSave.split("\\.")(1) else s"temp_$hiveTableNameToSave"
      //Saving the dataframe to HIVE
      if(isTempTableNeeded){
        dataframeToSave.write.mode(SaveMode.Overwrite).saveAsTable(tempHiveTable)
      }else{
        dataframeToSave.registerTempTable(tempHiveTable)
      }

      val overrideOrAppend = if(isOverwriteDataOk) "OVERWRITE" else "INTO"

      if(notEmpty(partitionColumns)){
        val partitionsCols = partitionColumns.split(",").map(_.trim).reduce(_+", "+_)
        sqlContext.setConf("hive.exec.dynamic.partition", "true")
        sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
        sqlContext.sql(s"INSERT $overrideOrAppend TABLE $hiveTableNameToSave PARTITION( $partitionsCols ) SELECT * FROM $tempHiveTable")
      }else{
        sqlContext.sql(s"INSERT $overrideOrAppend TABLE $hiveTableNameToSave SELECT * FROM $tempHiveTable")
      }
    }

    //Saving the dataframe to Hbase
    if(config != null && saveDataToHbase) {
      val hbaseContext = new HBaseContext(sc, config)

      HBaseDBUtil.putDataToHBase(hbaseContext,save_hbaseTableName, dataframeToSave, save_hbaseColumnFamily, save_numOfSalts)
    }
  }

  def getDataframeFromHiveORFile(sqlContext: SQLContext, isDataFromHive: String, sqlQueryOrCSVPath:String):DataFrame={

    var sqlQueryOrCSV: String = null
    if(isDataFromHive.equalsIgnoreCase("hive")) {
      sqlQueryOrCSV = sqlQueries.getString(sqlQueryOrCSVPath)
    }else{
      sqlQueryOrCSV = sqlQueryOrCSVPath
    }

    loadDataFrameFromSource(sqlContext, sqlQueryOrCSV, isDataFromHive)
  }

  def populateColumnFromHBase(dataframe:DataFrame, hbaseColLookup:String, hbaseTableName:String,
                              hbaseRowkey:String, hbaseColumnFamily:String):DataFrame={
    dataframe.withColumn(hbaseColLookup+"_hbaselookup", CommonUDFs.getHBaseData(lit(hbaseTableName), lit(hbaseRowkey), lit(hbaseColumnFamily), dataframe(hbaseColLookup)))
  }
}
