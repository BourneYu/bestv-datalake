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
  * Time: 15:34
  * Project: DataLake
  */
class OnlineLogExtractJob extends TRestClientInit with SparkSqlJob {

  var datalakeSubjectPGIDs = ""
  var datalakeSubjectName = ""

  override def runJob(sc: SQLContext, jobConfig: Config): Any = {
    val pgids = datalakeSubjectPGIDs.split("\\|", -1).toList
    val matchKey = super.getMatchKey(datalakeSubjectName)
    val timeKey = super.getTimeKey(datalakeSubjectName)
    val partition = super.getPartition(datalakeSubjectName, null)
    val initFileTotal = super.getFileTotal(datalakeSubjectName, null)
    val files = super.getOnlineFilesByPGIds(datalakeSubjectName, pgids)
    val dataType = super.getFileType(datalakeSubjectName)
    dataType.toLowerCase match {
      case "txt" => new SparkJobExecute(sc).startJobForOnlineTxtLog(datalakeSubjectName, files, matchKey, timeKey, partition, initFileTotal)
      case "avro" => new SparkJobExecute(sc).startJobForOnlineAvroLog(datalakeSubjectName, files, matchKey, timeKey, partition, initFileTotal)
      case "csv" => new SparkJobExecute(sc).startJobForOnlineCsvLog(datalakeSubjectName, files, matchKey, timeKey, partition, initFileTotal)
      case "json" => new SparkJobExecute(sc).startJobForOnlineJsonLog(datalakeSubjectName, files, matchKey, timeKey, partition, initFileTotal)
      case "td" => new SparkJobExecute(sc).startJobForOnlineTdLog(datalakeSubjectName, files, matchKey, timeKey, partition, initFileTotal)
      case "json_gbk" => new SparkJobExecute(sc).startJobForOnlineGbkJson(datalakeSubjectName, files, matchKey, timeKey, partition, initFileTotal)
      case _ =>
    }
  }

  override def validate(sc: SQLContext, config: Config): SparkJobValidation = {
    var validation = true
    val error_msgs = ArrayBuffer[String]()

    Try(config.getString("pgIds")) match {
      case Success(datalakePgIds) => this.datalakeSubjectPGIDs = datalakePgIds
      case Failure(ex) => {
        validation = false
        error_msgs += "not find input param datalakePgIds"
      }
    }

    Try(config.getString("subject")) match {
      case Success(datalakeSubject) => this.datalakeSubjectName = datalakeSubject
      case Failure(ex) => {
        validation = false
        error_msgs += "not find input param datalakeSubject"
      }
    }

    if (validation) SparkJobValid else SparkJobInvalid(error_msgs.mkString(";"))
  }
}

