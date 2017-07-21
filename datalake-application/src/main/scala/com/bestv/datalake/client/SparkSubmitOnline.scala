package com.bestv.datalake.client

import com.bestv.datalake.spark.TRestClientInit
import org.apache.log4j.PropertyConfigurator
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.slf4j.{Logger, LoggerFactory}

/**
  * Package: com.bestv.datalake.spark. 
  * User: SichengWang 
  * Date: 2016/6/12 
  * Time: 12:16  
  * Project: DataLake
  */
object SparkSubmitOnline extends TRestClientInit {
  def main(args: Array[String]) {
    val sc: SparkContext = new SparkContext()
    val sqlContext: SQLContext = new SQLContext(sc)
    //    val hiveContext = new HiveContext(sc)
    PropertyConfigurator.configure("log4j.properties")
    val logger: Logger = LoggerFactory.getLogger("stan")
    val datalakeSubjectName = args(0)
    val datalakeSubjectStatus = args(1)
    val matchKey = super.getMatchKey(datalakeSubjectName)
    val timeKey = super.getTimeKey(datalakeSubjectName)
    val partition = super.getPartition(datalakeSubjectName, null)
    val initFileTotal = super.getFileTotal(datalakeSubjectName, null)
//    val files = super.getOnlineFiles(datalakeSubjectName, datalakeSubjectStatus)
    //    new SparkJobExecute(sc).startJobForOnlineLog(datalakeSubjectName, files.toList, matchKey, timeKey, partition, initFileTotal)
  }
}
