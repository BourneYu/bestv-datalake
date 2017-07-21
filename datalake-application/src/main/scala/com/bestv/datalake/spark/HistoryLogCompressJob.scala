package com.bestv.datalake.spark

import com.typesafe.config.Config
import org.apache.spark.sql.SQLContext
import spark.jobserver._

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

/**
  * Package: com.bestv.datalake.spark. 
  * User: SichengWang 
  * Date: 2016/6/14
  * Time: 15:35  
  * Project: DataLake
  */
class HistoryLogCompressJob extends TRestClientInit with SparkSqlJob {

  var subject = ""
  var partitionName = ""

  override def runJob(sc: SQLContext, jobConfig: Config): Any = {
    val initFileTotal = super.getFileTotal(subject, null)
    val partitions = super.getPartitionsByPartitionName(subject,partitionName)
    partitions.foreach(aPartition=>{
      val partition = aPartition.getPartition
      val vparts = super.getPartitionByPartitionId(aPartition.getPartitionId)
      val primitPaths = for(aVpart <- vparts) yield aVpart.getPath
      new SparkJobExecute(sc).startJobForHistoryLog(subject, partition, primitPaths, initFileTotal)})
  }

  override def validate(sc: SQLContext, config: Config): SparkJobValidation = {
    var validation = true
    val error_msgs = ArrayBuffer[String]()

    Try(config.getString("subject")) match {
      case Success(datalakeSubject) => this.subject = datalakeSubject
      case Failure(ex) => {
        validation = false
        error_msgs += "not find input param datalakeSubject"
      }
    }

    Try(config.getString("pgIds")) match {
      case Success(partitionName) => this.partitionName = partitionName
      case Failure(ex) => {
        validation = false
        error_msgs += "not find input param datalakePgIds"
      }
    }

    if (validation) SparkJobValid else SparkJobInvalid(error_msgs.mkString(";"))
  }
}
