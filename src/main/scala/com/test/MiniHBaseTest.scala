package com.test

import java.io.PrintWriter
import com.common.utils.CommonUtilities._
import com.common.utils.Configuration._
import com.common.utils.PullHive_HBaseData
import com.common.utils.db.HBaseDBUtil
import com.minicluster.hbase.{HBaseContext, HBaseMiniClusterCommon}
import org.apache.hadoop.hbase.client.{Get, Result, _}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseTestingUtility}
import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.MapType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}

import scala.collection.mutable

class MiniHBaseTest extends FunSuite with BeforeAndAfterEach with BeforeAndAfterAll {

  var htu: HBaseTestingUtility = null

  val custAttrTableName = "custAttr"
  val custTSTableName = "custTS"
  val hbaseTempTable = "hbaseTempTable"
  val columnFamily = "c"
  val tableName = "t1"

  var sc:SparkContext = getSparkContext("local", "HBase Context Testing")
  val sqlContext = new HiveContext(sc)
  import sqlContext.implicits._
  var hbaseContext: HBaseContext = null

  override def beforeAll() {
    HBaseMiniClusterCommon.startHbaseCluster(sc)
    HBaseMiniClusterCommon.deleteTable(custAttrTableName)
    HBaseMiniClusterCommon.createTable(custAttrTableName, columnFamily)
    HBaseMiniClusterCommon.deleteTable(custTSTableName)
    HBaseMiniClusterCommon.createTable(custTSTableName, columnFamily)
    HBaseMiniClusterCommon.deleteTable(hbaseTempTable)
    HBaseMiniClusterCommon.createTable(hbaseTempTable, columnFamily)
    HBaseMiniClusterCommon.deleteTable(tableName)
    HBaseMiniClusterCommon.createTable(tableName, columnFamily)

    htu = HBaseMiniClusterCommon.htu

    hbaseContext = new HBaseContext(sc, HBaseMiniClusterCommon.config);

    insertDatatoHbase(rddForCustAttr(sc), custAttrTableName, columnFamily)
  }

  override def afterAll() {
    HBaseMiniClusterCommon.stopHbaseCluster(sc)
  }

  test("Test run") {

    setLogLevels(Level.INFO, Seq("spark", "org", "akka"))
    val out = new PrintWriter("C:\\Users\\seeram\\Documents\\Workspace\\Print.log")

    def getCustIDStatus(custID:String):String={
      ((Math.random()*100).asInstanceOf[Int]%2).toString
    }

    val dateHourMinsSecPatternInt = "yyyyMMddHHmmss"
    val TS_UDF = udf[String, String]((CustID: String) => CustID + ":" +new DateTime().toString(DateTimeFormat.forPattern(dateHourMinsSecPatternInt)))

    def insertRecordsIntoTSTable(custAttrTable:DataFrame)={
      val TSRecordsDF = custAttrTable.withColumn("CustID_TS", TS_UDF($"CustID")).drop("CustID").select("CustID_TS", "Status")
      val rddTSRecords = convertDataFrameToHBaseRecords(TSRecordsDF, columnFamily,0,"CustID_TS")
      insertDatatoHbase(rddTSRecords, custTSTableName, columnFamily)
    }

    val loopCounter = 10
    val sleepPerRecord = 1000

    //Caches the Customer Attribute table as in memory DF
    var custAttrInMemDF = scanRecordsFromHBase(custAttrTableName, "CustID").cache()
    out.println("Original Cust Attr Data in Hbase ######################")
    custAttrInMemDF.collect().foreach(out.println(_))

    val updatedStatusUDF = udf[String, String, String]((CustID: String, oldStatus:String) => {
      val newStatus =  ((Math.random()*100).asInstanceOf[Int]%2).toString//getCustIDStatus(CustID)
      if(!oldStatus.equalsIgnoreCase(newStatus)) {
        newStatus
      } else oldStatus
    })

    insertRecordsIntoTSTable(custAttrInMemDF)

    for(i <- 1 until loopCounter) {

      val updatedInMemCustAttrDF = custAttrInMemDF.withColumn("Status_updated", updatedStatusUDF($"CustID", $"Status")).drop("Status").withColumnRenamed("Status_updated","Status").cache()

      val custAttrToUpdateHbase = updatedInMemCustAttrDF.except(custAttrInMemDF).select("CustID", "Status").cache()
      out.println("###########Records updated in this Iterateion ############### "+i)
      custAttrToUpdateHbase.collect().foreach(out.println(_))
      out.println("############################################################# "+i)
      val rddCustAttrToUpdateHbase = convertDataFrameToHBaseRecords(custAttrToUpdateHbase, columnFamily, 0, "CustID")
      insertDatatoHbase(rddCustAttrToUpdateHbase, custAttrTableName, columnFamily)

      custAttrInMemDF.unpersist()
      custAttrInMemDF = updatedInMemCustAttrDF.cache()

      insertRecordsIntoTSTable(custAttrToUpdateHbase)
      custAttrToUpdateHbase.unpersist()

      Thread.sleep(sleepPerRecord)

      out.println("Customer Attr Table ###############")
      scanRecordsFromHBase(custAttrTableName, "CustID").collect().foreach(out.println(_))

      out.println("Customer TS Table ###############")
      scanRecordsFromHBase(custTSTableName, "CustID_TS").collect().foreach(out.println(_))

      out.println("EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEENNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD loop "+i)
    }

    out.flush()
    out.close()
  }

  def insertDatatoHbase(rdd:RDD[(Array[Byte],Array[(Array[Byte],Array[Byte],Array[Byte])])], tableName:String, columnFamily:String) ={

    if(rdd.first() != null) {
      hbaseContext.bulkPut[(Array[Byte], Array[(Array[Byte], Array[Byte], Array[Byte])])](rdd,
        tableName,
        (putRecord) => {

          val put = new Put(putRecord._1)
          putRecord._2.foreach((putValue) => put.add(putValue._1, putValue._2, putValue._3))
          put

        },
        true);
    }else{
      println("RDD had no records to insert......")
    }
  }

  def scanRecordsFromHBase(tableName: String, rowkeyIDColName:String):DataFrame={

    val scan = new Scan();
    scan.setCaching(100);
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

    val getRdd = hbaseContext.hbaseRDD[(String, Row)](tableName, scan, convertResultToRow)
    sqlContext.createDataFrame(getRdd.map(_._2), createSchemaFromStrDelValues(getRdd.keys.first))
  }


  def getRecordsFromHBase(rdd:RDD[String], tableName: String):DataFrame={

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

    val getRdd = hbaseContext.bulkGet[Array[Byte], Record](tableName, 2, getRecords, record => new Get(record), convertResultToRow)
    sqlContext.createDataFrame(getRdd.map(_._2), createSchemaFromStrDelValues(getRdd.keys.first))
  }

  def convertDataFrameToHBaseRecords(dataframe:DataFrame, colFamilyName:String, numOfSalts:Int, colForRowKey:String = null):RDD[(Array[Byte],Array[(Array[Byte],Array[Byte],Array[Byte])])]={

    val columnFamily = Bytes.toBytes(colFamilyName)

    dataframe.map(row=>{

      if (row != null) {
        val rowFields = row.schema.fields

        var putRecord = Array[(Array[Byte], Array[Byte], Array[Byte])]()
        val colForRowKeyColName = if(colForRowKey == null) rowFields(0).name else colForRowKey
        val rowID = Bytes.toBytes(row.getAs[Any](colForRowKeyColName).toString)

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
    }).filter(_!=null)
  }

  def rddForCustAttr(sc: SparkContext)={
    sc.parallelize(Array(
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("A")))),
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("A Addr")))),
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("B")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("B Addr")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("C")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("C Addr")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("D")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("D Addr")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("E")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("E Addr")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("F")))),
      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("F Addr")))),
      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("7"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("G")))),
      (Bytes.toBytes("7"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("G Addr")))),
      (Bytes.toBytes("7"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("8"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("H")))),
      (Bytes.toBytes("8"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("H Addr")))),
      (Bytes.toBytes("8"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("9"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("I")))),
      (Bytes.toBytes("9"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("I Addr")))),
      (Bytes.toBytes("9"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("10"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("J")))),
      (Bytes.toBytes("10"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("J Addr")))),
      (Bytes.toBytes("10"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("11"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("K")))),
      (Bytes.toBytes("11"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("K Addr")))),
      (Bytes.toBytes("11"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("12"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Name"), Bytes.toBytes("L")))),
      (Bytes.toBytes("12"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Addr"), Bytes.toBytes("L Addr")))),
      (Bytes.toBytes("12"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0"))))
    ))
  }


  def rddForCustTS(sc: SparkContext)={

    val date = new DateTime().toString()
    sc.parallelize(Array(
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("7"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("7"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("8"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("8"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("9"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("9"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("10"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("10"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("11"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("11"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0")))),

      (Bytes.toBytes("12"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("TS"), Bytes.toBytes(date)))),
      (Bytes.toBytes("12"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Status"), Bytes.toBytes("0"))))
    ))
  }

  test("Testing to check if Map data type is inserted as seperate columns in Hbase"){

    var custAttrInMemDF = scanRecordsFromHBase(custAttrTableName, "CustID").cache()
    custAttrInMemDF.registerTempTable("temp")
    val temp_new = sqlContext.sql("select t.*, map('NameKey', t.Name, 'testkey1', 'testvalue1', 'testkey2', 'testvalue2', 'testkey3', 'testvalue3') as mapCol from temp t")
    temp_new.show()
    temp_new.printSchema()

    val rddTSRecords = convertDataFrameToHBaseRecords(temp_new, columnFamily,0,null)
    insertDatatoHbase(rddTSRecords, hbaseTempTable, columnFamily)

    val hbaseTempTable_new = scanRecordsFromHBase(hbaseTempTable, "rowKeyId").cache()
    hbaseTempTable_new.printSchema()
    hbaseTempTable_new.show()
  }


  test("Load file data and populate new column from Hbase to Dataframe") {

    val args = "clusterORLocal=l hiveORFile=csv sqlQueryOrCSVPath=data/adult.csv tableNameIfAny=applications  sparkOrHiveContext=hive dataLocationToSave=tmp hbaseConfigXML= hbaseTableName=t1 hbaseRowkey=1 hbaseColumnFamily=c hbaseColLookup=native_country"

    val namedArgs = getNamedArgs(args.split(" "))
    val runLocal = namedArgs("clusterORLocal").equalsIgnoreCase("l")
    val isDataFromHive = namedArgs("hiveORFile")
    var sqlQueryOrCSVPath = namedArgs("sqlQueryOrCSVPath")
    val tableNameIfAny:String = namedArgs("tableNameIfAny")
    val sparkOrHiveContext:String = namedArgs("sparkOrHiveContext")
    val dataLocationToSave = namedArgs("dataLocationToSave")
    val hbaseConfigXML:String = namedArgs("hbaseConfigXML")
    val hbaseTableName = namedArgs("hbaseTableName")
    val hbaseRowkey = namedArgs("hbaseRowkey")
    val hbaseColumnFamily = namedArgs("hbaseColumnFamily")
    val hbaseColLookup = namedArgs("hbaseColLookup")

    val sqlContext = new SQLContext(sc);

    val dataframe = loadDataFrameFromSource(sqlContext, sqlQueryOrCSVPath, isDataFromHive)

    insertDatatoHbase(rddToInsertValuesForUDFTest(sc), tableName, columnFamily)
    HBaseDBUtil.connection = HBaseMiniClusterCommon.connection

    PullHive_HBaseData.populateColumnFromHBase(dataframe:DataFrame, hbaseColLookup:String, hbaseTableName:String, hbaseRowkey:String, hbaseColumnFamily:String)
      .show()

  }

  test("Pull data from file and populate new column from Hbase to Dataframe") {

    //clusterORLocal=l hiveORFile=file sqlQueryOrCSVPath=data/mllib/adult.csv tableNameIfAny=temptable  sparkOrHiveContext=hive hiveTableName= partitionColumns= isOverwriteDataOk=y dataLocationToSave=tmp hbaseConfigXML= populateColValFromHBase= udf_hbaseTableName=t1 udf_hbaseRowkey=1 udf_hbaseColumnFamily=c udf_hbaseColLookup=native_country saveDataToHbase=y save_hbaseTableName=testhbasetable save_hbaseRowkey=1 save_hbaseColumnFamily=c save_numOfSalts=6
    val args = "clusterORLocal=l hiveORFile=csv sqlQueryOrCSVPath=data/adult.csv tableNameIfAny=temptable sparkOrHiveContext=spark dataLocationToSave=tmp hbaseConfigXML= hbaseTableName=t1 hbaseRowkey=2 hbaseColumnFamily=c hbaseColLookup=native_country"

    val namedArgs = getNamedArgs(args.split(" "))
    val runLocal = namedArgs("clusterORLocal").equalsIgnoreCase("l")
    val isDataFromHive = namedArgs("hiveORFile")
    var sqlQueryOrCSVPath = namedArgs("sqlQueryOrCSVPath")
    val tableNameIfAny:String = namedArgs("tableNameIfAny")
    val sparkOrHiveContext:String = namedArgs("sparkOrHiveContext")
    val dataLocationToSave = namedArgs("dataLocationToSave")
    val hbaseConfigXML:String = namedArgs("hbaseConfigXML")
    val hbaseTableName = namedArgs("hbaseTableName")
    val hbaseRowkey = namedArgs("hbaseRowkey")
    val hbaseColumnFamily = namedArgs("hbaseColumnFamily")
    val hbaseColLookup = namedArgs("hbaseColLookup")

    val sqlContext = new SQLContext(sc);

    val dataframe = PullHive_HBaseData.getDataframeFromHiveORFile(sqlContext, isDataFromHive, sqlQueryOrCSVPath).limit(10)
    dataframe.registerTempTable(tableNameIfAny)

    println("dataframe returned from hive or CSV file")
    dataframe.show(20)

    insertDatatoHbase(rddToInsertValuesForUDFTest(sc), tableName, columnFamily)
    HBaseDBUtil.connection = HBaseMiniClusterCommon.connection

    val dataframePulled = PullHive_HBaseData.populateColumnFromHBase(dataframe:DataFrame, hbaseColLookup:String, hbaseTableName:String, hbaseRowkey:String, hbaseColumnFamily:String).limit(10)

    dataframePulled.show()

    val hbaseContext = new HBaseContext(sc, HBaseMiniClusterCommon.config);

    val dataToHbase = HBaseDBUtil.convertDataFrameToHBaseRecords(dataframePulled,hbaseColumnFamily, 6)

    insertDatatoHbase(dataToHbase, tableName, columnFamily)
  }

  def rddToInsertValuesForUDFTest(sc: SparkContext)={
    sc.parallelize(Array(
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("United-States"), Bytes.toBytes("-USA-")))),
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Cuba"), Bytes.toBytes("-USA-")))),
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Jamaica"), Bytes.toBytes("-USA-")))),
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Mexico"), Bytes.toBytes("-USA-")))),
      (Bytes.toBytes("1"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Dominican-Republic"), Bytes.toBytes("-US-")))),

      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("United-States"), Bytes.toBytes("-USA-")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Cuba"), Bytes.toBytes("-CUBA-")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Jamaica"), Bytes.toBytes("-JAMICA-")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Mexico"), Bytes.toBytes("-MEXI-")))),
      (Bytes.toBytes("2"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Dominican-Republic"), Bytes.toBytes("-DR-")))),

      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("United-States"), Bytes.toBytes("-USA-")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Cuba"), Bytes.toBytes("-CUBA-")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Jamaica"), Bytes.toBytes("-JAMICA-")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Mexico"), Bytes.toBytes("-MEXI-")))),
      (Bytes.toBytes("3"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Dominican-Republic"), Bytes.toBytes("-DR-")))),

      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("United-States"), Bytes.toBytes("-USA-")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Cuba"), Bytes.toBytes("-CUBA-")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Jamaica"), Bytes.toBytes("-JAMICA-")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Mexico"), Bytes.toBytes("-MEXI-")))),
      (Bytes.toBytes("4"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Dominican-Republic"), Bytes.toBytes("-DR-")))),

      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("United-States"), Bytes.toBytes("-USA-")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Cuba"), Bytes.toBytes("-CUBA-")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Jamaica"), Bytes.toBytes("-JAMICA-")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Mexico"), Bytes.toBytes("-MEXI-")))),
      (Bytes.toBytes("5"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Dominican-Republic"), Bytes.toBytes("-DR-")))),

      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("United-States"), Bytes.toBytes("-USA-")))),
      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Cuba"), Bytes.toBytes("-CUBA-")))),
      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Jamaica"), Bytes.toBytes("-JAMICA-")))),
      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Mexico"), Bytes.toBytes("-MEXI-")))),
      (Bytes.toBytes("6"), Array((Bytes.toBytes(columnFamily), Bytes.toBytes("Dominican-Republic"), Bytes.toBytes("-DR-"))))
    ))
  }
}
