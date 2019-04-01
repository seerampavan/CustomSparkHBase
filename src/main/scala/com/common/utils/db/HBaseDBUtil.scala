package com.common.utils.db

import java.io.File
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.collection.mutable
import com.common.utils.CommonUtilities

object HBaseDBUtil {

  @transient var connection : Connection = null
  @transient var config : Configuration = null

  /**
    * Function to get the Hbase Connection
    *
    * @param hbaseConfigXML
    * @return
    */
  def getHBaseDBConfiguration(hbaseConfigXML:String):Configuration={
    if(connection == null || config == null){
      try {
        config = HBaseConfiguration.create()
        config.addResource(new File(hbaseConfigXML).toURI.toURL)
        HBaseAdmin.checkHBaseAvailable(config);
        println("HBase is running!");
        connection = ConnectionFactory.createConnection(config)
      }catch {
        case e:Exception => {
          e.printStackTrace()
        }
      }
    }
    config
  }

  /**
    * Function to get the data from Hbase
    *
    * @param tableName
    * @param rowkey
    * @param columnFamily
    * @param columnQualifier
    * @return
    */
  def getDataFromHbase(tableName:String, rowkey:String, columnFamily:String, columnQualifier: String):String={
    val htable = connection.getTable(TableName.valueOf(tableName))

    var data =
      if(columnQualifier == null || columnQualifier.isEmpty){
        null
      }else {
        val value = htable.get(new Get(Bytes.toBytes(rowkey))).getColumnLatest(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier.trim))
        if(value != null ) Bytes.toString(value.getValue()) else null
      }
    data
  }

  /**
    * To get column value from the column qualifier
    *
    * @param result
    * @param columnFamily
    * @param column
    * @return
    */
  def getColumnFromResult(result:Result, columnFamily:String, column:Array[Byte]): String = {
    val cell = result.getColumnLatestCell(Bytes.toBytes(columnFamily), column)
    Bytes.toString(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }

  /**
    * for generating Hbase put for each row dataframe
    *
    * @param row
    * @param fieldList
    * @param columnFamily
    * @param numOfSalts
    * @return
    */
  def generatePutToHBase(row: Row, fieldList:Array[String], columnFamily: Array[Byte], numOfSalts:Int): Put = {
    val put = new Put(generateHbaseRowKey(row.get(0).toString, numOfSalts))
    for(i <- 1 until fieldList.length){
      put.addColumn(columnFamily, Bytes.toBytes(fieldList(i)), Bytes.toBytes(row.get(i).toString))
    }
    put
  }

  /**
    * Function to prepare the salted rowkey for Hbase table
    *
    * @param id
    * @param numOfSalts
    * @return
    */
  def generateHbaseRowKey(id:String, numOfSalts:Int): Array[Byte] = {
    val salt = StringUtils.leftPad(Math.abs(id.hashCode % numOfSalts).toString, 4, "0")
    Bytes.toBytes(salt + ":" + id)
  }

  /**
    * Put Data to HBase
    *
    * @param hbaseContext
    * @param hbaseTblName
    * @param dataframe
    * @param colFamilyName
    * @param numOfSalts
    * @tparam T
    */
  def putDataToHBase[T](hbaseContext:HBaseContext, hbaseTblName:String, dataframe:DataFrame, colFamilyName:String, numOfSalts:Int)={
    if(dataframe.first() != null) {
      val dataToHbase = convertDataFrameToHBaseRecords(dataframe,colFamilyName, numOfSalts)

      dataToHbase.hbaseBulkPut(hbaseContext, TableName.valueOf(hbaseTblName), putRecord => {
        val put = new Put(putRecord._1)
        putRecord._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
        put
      })
    }else{
      println("RDD had no records to insert......")
    }
  }

  /**
    *  Convert DataFrame to HBase Records
    *
    * @param dataframe
    * @param colFamilyName
    * @param numOfSalts
    * @param colForRowKey
    * @return
    */
  def convertDataFrameToHBaseRecords(dataframe:DataFrame, colFamilyName:String, numOfSalts:Int, colForRowKey:String = null):RDD[(Array[Byte],Array[(Array[Byte],Array[Byte],Array[Byte])])]={

    val columnFamily = Bytes.toBytes(colFamilyName)

    dataframe.map(row=>{

      if (row != null) {
        val rowFields = row.schema.fields

        var putRecord = Array[(Array[Byte], Array[Byte], Array[Byte])]()
        val colForRowKeyColName = if(colForRowKey == null) rowFields(0).name else colForRowKey
        val rowID = HBaseDBUtil.generateHbaseRowKey(row.getAs[Any](colForRowKeyColName).toString, numOfSalts)

        rowFields.foreach(sf => {
          val colType = sf.dataType
          val colName: String = sf.name
          val colValue = row.getAs[Any](colName)

          if(!colForRowKeyColName.equalsIgnoreCase(colName)) {
            if (colType.isInstanceOf[MapType]) {
              val mapData = row.getAs[Map[Any, Any]](colName)
              for (mapElem <- mapData) {
                putRecord = putRecord :+(columnFamily, Bytes.toBytes(mapElem._1.toString), Bytes.toBytes(if (mapElem._2 != null) mapElem._2.toString else ""))
              }
            } else {
              putRecord = putRecord :+(columnFamily, Bytes.toBytes(colName), Bytes.toBytes(if (colValue != null) colValue.toString else ""))
            }
          }
        })

        (rowID, putRecord)
      } else null
    }).filter(_!=null).repartition(10)
  }

  /**
    * Get Records from HBase
    *
    * @param sqlContext
    * @param hbaseContext
    * @param rdd
    * @param tableName
    * @param batchSize
    * @return
    */
  def getRecordsFromHBase(sqlContext:SQLContext, hbaseContext:HBaseContext, rdd:RDD[String], tableName: String, batchSize:Int):DataFrame={

    type Record = (String, Row)
    val convertResultToRow: (Result) => Record = (result:Result) => {
      var colQualifiers = mutable.LinkedHashSet("CustID")
      val it = result.listCells().iterator()
      var record = Array[String](Bytes.toString(result.getRow))

      while (it.hasNext) {
        val cell = it.next()
        colQualifiers += Bytes.toString(CellUtil.cloneQualifier(cell))
        record = record :+ Bytes.toString(CellUtil.cloneValue(cell))
      }
      val cols:String = colQualifiers.reduce(_+","+_)
      (cols, Row.fromSeq(record))
    }

    val getRecords:RDD[Array[Byte]] = rdd.map(Bytes.toBytes(_))

    val getRdd = getRecords.hbaseBulkGet(hbaseContext, TableName.valueOf(tableName), batchSize, record => new Get(record), convertResultToRow)
    sqlContext.createDataFrame(getRdd.map(_._2), CommonUtilities.createSchemaFromStrDelValues(getRdd.keys.first))
  }

  /**
    * Scan Records From HBase
    *
    * @param sqlContext
    * @param hbaseContext
    * @param tableName
    * @param rowkeyIDColName
    * @return
    */
  def scanRecordsFromHBase(sqlContext:SQLContext, hbaseContext:HBaseContext, scan:Scan, tableName: String, rowkeyIDColName:String):DataFrame={

    scan.setCaching(100)
    type Record = (String, Option[String], Option[String])

    val convertResultToRow: ((ImmutableBytesWritable, Result)) => (String, Row) = (data:(ImmutableBytesWritable,Result)) => {
      var colQualifiers = mutable.LinkedHashSet(rowkeyIDColName)
      val (ibw, result) = data
      val it = result.listCells().iterator()
      var record = Array[String](Bytes.toString(result.getRow))

      while (it.hasNext) {
        val cell = it.next()
        colQualifiers += Bytes.toString(CellUtil.cloneQualifier(cell))
        record = record :+ Bytes.toString(CellUtil.cloneValue(cell))
      }
      val cols:String = colQualifiers.reduce(_+","+_)
      (cols, Row.fromSeq(record))
    }

    val getRdd = hbaseContext.hbaseRDD[(String, Row)](TableName.valueOf(tableName), scan, convertResultToRow)
    sqlContext.createDataFrame(getRdd.map(_._2), CommonUtilities.createSchemaFromStrDelValues(getRdd.keys.first))
  }
}
