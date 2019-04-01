package com.common.utils

import java.sql.Timestamp
import org.apache.commons.lang.exception.ExceptionUtils._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import Configuration._

object CommonUtilities {
  val datePatternMonthInt = "YYYYMM"

  def createDate(input: String, datePattern: String): DateTime = {
    DateTimeFormat.forPattern(datePattern).parseDateTime(input)
  }

  def convertTimestampToIntPattern(date: Timestamp, datePattern: String): Long = {
    new DateTime(date.getTime).toString(DateTimeFormat.forPattern(datePattern)).toLong
  }

  def getNamedArgs(args: Array[String]): Map[String, String] = {
    println(s"################ Input parameters are ################ ${args.toList}")
    args.filter(_.contains("=")) // take only named arguments
      .map { x =>
      val Array(key, value) = x.split("=", 2)
      (key, if (value == null || value.isEmpty) null else value)
    }.toMap.withDefaultValue(null) //convert to a map
  }

  def createSchemaFromStrDelValues(baseSchema: String): StructType = {
    StructType(baseSchema.split(",").map(f => StructField(f, StringType, true)))
  }

  def pullDataFromCSVFile(sqlContext: SQLContext, isHeaderExist: Boolean, filePath: String, delimiter: String, csvSplit: String): DataFrame = {
    var csvDataFrame: DataFrame = null
    try {
      if (isHeaderExist) {
        csvDataFrame = sqlContext.read
          .format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(filePath)
      } else {
        if (csvSplit != null) {
          val schema = createSchemaFromStrDelValues(csvSplit)
          csvDataFrame = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "false")
            .option("delimiter", delimiter)
            .option("inferSchema", "false")
            .schema(schema)
            .load(filePath)
        }
      }
    } catch {
      case ex: Exception =>
        Console.err.println(s"Unable to read the CSV file from the location $filePath ${getStackTrace(ex)}")
        throw ex
    }
    csvDataFrame
  }

  def saveDataToCSVFile(dataframe: DataFrame, locationToSave: String, delimiter: String, isHeaderNeeded: String): Unit = {
    try {
      dataframe.write.mode(SaveMode.Overwrite)
        .format("com.databricks.spark.csv")
        .option("header", isHeaderNeeded)
        .option("delimiter", delimiter)
        .save(locationToSave)
    } catch {
      case ex: Exception =>
        Console.err.println(s"Unable to save the CSV files to location $locationToSave ${getStackTrace(ex)}")
        throw ex
    }
  }

  def checkFilesExistsAndReturnDF(sqlContext: SQLContext, fileType: String, filePath: String): DataFrame = {
    var dataframe: DataFrame = null
    try {
      if ("parquet".equalsIgnoreCase(fileType)) {
        dataframe = sqlContext.read.load(filePath)
      } else if ("json".equalsIgnoreCase(fileType)) {
        dataframe = sqlContext.read.json(filePath)
      }
    } catch {
      case ex: Throwable =>
        Console.err.println(s"$fileType unable to read the file from the location $filePath ${getStackTrace(ex)}")
    }
    dataframe
  }

  def checkFilesExistsInPathsAndReturnDF(sqlContext: SQLContext, fileType: String, filePaths: String*): DataFrame = {
    var dataframe: DataFrame = null
    try {
      if ("parquet".equalsIgnoreCase(fileType)) {
        sqlContext.read.load(filePaths: _*)
      } else if ("json".equalsIgnoreCase(fileType)) {
        sqlContext.read.json(filePaths: _*)
      }
    } catch {
      case ex: Throwable =>
        println(s"$fileType unable to read the file from the locations $filePaths")
        ex.printStackTrace()
        for (filePath <- filePaths) {
          val tempDataFrame = checkFilesExistsAndReturnDF(sqlContext, fileType, filePath)
          if (tempDataFrame != null) {
            if (dataframe != null) {
              dataframe = dataframe.unionAll(tempDataFrame)
            } else {
              dataframe = tempDataFrame
            }
          }
        }
    }
    dataframe
  }

  def loadDataFrameFromSource(sqlContext: SQLContext, sqlQueryOrCSVPath: String, isDataFromHive: String="hive"): DataFrame = {
    if (isDataFromHive.equalsIgnoreCase("hive")) {
      sqlContext.sql(sqlQueryOrCSVPath)
    } else if (isDataFromHive.equalsIgnoreCase("csv")) {
      pullDataFromCSVFile(sqlContext, true, sqlQueryOrCSVPath, null, null)
    } else {
      checkFilesExistsInPathsAndReturnDF(sqlContext, "parquet", sqlQueryOrCSVPath)
    }
  }

  def getDataframeFromHiveORFile(sqlContext: SQLContext, sqlQueryOrCSVPath: String, isDataFromHive: String="hive"): DataFrame = {
    val sqlQueryOrCSV =
      if (isDataFromHive.equalsIgnoreCase("hive")) {
        sqlQueries.getString(sqlQueryOrCSVPath)
      } else {
        sqlQueryOrCSVPath
      }
    loadDataFrameFromSource(sqlContext, sqlQueryOrCSV, isDataFromHive)
  }

  def getSparkContext(runLocal: String, appName: String): SparkContext = {
    val sc = if (runLocal.equalsIgnoreCase("local") || runLocal.equalsIgnoreCase("l")) {
      val sparkConfig = new SparkConf()
      new SparkContext("local[*]", appName, sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName(appName)
      SparkContext.getOrCreate(sparkConfig)
    }
    sc.hadoopConfiguration.setBoolean("parquet.enable.summary-metadata", false)
    sc
  }

  def cleanDataFrame(df: DataFrame, listOfColumns: String = null): DataFrame = {
    val listOfColumnsToClean =
      if (notEmpty(listOfColumns)) {
        listOfColumns.split(",").filter((!_.equals("null")))
      } else {
        df.columns
      }
    listOfColumnsToClean.foldRight(df)((columnName, df) => df.filter(s"trim($columnName) <> 'null'"))
  }

  def cleanDataFrameWithDefaults(sqlContext: SQLContext, df: DataFrame, listOfColumns: String = null): DataFrame = {
    val listOfColumnsToClean =
      if (notEmpty(listOfColumns)) {
        listOfColumns.split(",").filter((!_.equals("null")))
      } else {
        df.columns
      }
    listOfColumnsToClean.foldRight(df)((columnName, df) =>
      df.withColumn(columnName + "_defaults", when(df(columnName).isNull, 0.0).otherwise(df(columnName))).drop(df(columnName)).withColumnRenamed(columnName + "_defaults", columnName)
    )
  }

  def notEmpty(field: String): Boolean = field != null && !field.isEmpty

  def setLogLevels(level: Level, loggers: Seq[String]): Unit = {
    loggers.foreach(loggerName => Logger.getLogger(loggerName).setLevel(level))
    println(s"Running the application in $level")
  }

  def dfWithRowIndexUsingRDD(df: DataFrame, offset: Int = 1, colName: String = "row_num", inFront: Boolean = true): DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map { case (value, index) =>
        val rowNum = index + offset
        Row.fromSeq(if (inFront) value +: value.toSeq else value.toSeq :+ rowNum)
      },
      StructType(
        if (inFront) {
          StructField(colName, LongType, false) +: df.schema.fields
        } else {
          df.schema.fields :+ StructField(colName, LongType, false)
        }
      )
    )
  }

  def dfWithRowIndexUsingDF(df: DataFrame, offset: Int = 1, colName: String = "row_num"): DataFrame = {
    df.withColumn(colName, row_number.over(Window.partitionBy(lit(1)).orderBy(lit(offset))))
  }

  def createHiveExternalTableWithNoPartitions(sqlContext:SQLContext, filePath:String, fileType:String, hiveTableName:String, isOverwriteDataOk:Boolean=true)={

    if(filePath != null && !filePath.isEmpty){
      val dfForHive = CommonUtilities.checkFilesExistsAndReturnDF(sqlContext,"parquet", filePath).limit(1)
        .write.mode(if(isOverwriteDataOk) SaveMode.Overwrite else SaveMode.Append)

      dfForHive.saveAsTable(hiveTableName)
      sqlContext.sql(s"ALTER TABLE $hiveTableName SET TBLPROPERTIES('EXTERNAL'='TRUE')")
      sqlContext.sql(s"ALTER TABLE $hiveTableName SET LOCATION '$filePath'")
    }
    sqlContext.sql(s"MSCK REPAIR TABLE $hiveTableName")
  }
}