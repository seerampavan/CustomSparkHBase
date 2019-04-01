package com.minicluster.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext

object HBaseMiniClusterCommon {

  var htu: HBaseTestingUtility = null

  var config : Configuration= null
  var connection : Connection = null;

  def startHbaseCluster(sc: SparkContext)={
    htu = HBaseTestingUtility.createLocalHTU()

    htu.cleanupTestDir()
    println("starting com.minicluster")
    htu.startMiniZKCluster();
    htu.startMiniHBaseCluster(1, 1);
    println(" - com.minicluster started")

    config = htu.getConfiguration
    connection = ConnectionFactory.createConnection(config)
    println("Got Connection------------")
  }

  def stopHbaseCluster(sc: SparkContext)={
    println("shuting down com.minicluster")
    htu.shutdownMiniHBaseCluster()
    htu.shutdownMiniZKCluster()
    println(" - com.minicluster shut down")
    htu.cleanupTestDir()
    sc.stop();
  }

  def createTable(tableName:String, columnFamily:String)={
    println(" - creating table " + tableName)
    htu.createTable(Bytes.toBytes(tableName), Bytes.toBytes(columnFamily))
    println(" - created table")
  }

  def deleteTable(tableName:String)={
    try {
      htu.deleteTable(Bytes.toBytes(tableName))
    } catch {
      case e: Exception => {
        println(" - no table " + tableName + " found")
      }
    }
  }
}
