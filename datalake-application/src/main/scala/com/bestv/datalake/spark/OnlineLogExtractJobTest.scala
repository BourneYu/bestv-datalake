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
class OnlineLogExtractJobTest extends SparkSqlJob {

  var dlSubject: Option[String] = None
  var dlFiles : Option[String] = None

  override def runJob(sc: SQLContext, jobConfig: Config): Any = {
    val files = List[String](dlFiles.get)
    val partition = "yyyy/MM/dd/HH"
    val matchKey = "header.hashcode"
    val timeKey = "if(header.time='',from_unixtime(unix_timestamp(),'yyyy/MM/dd/HH'),from_unixtime(substring(header.time,1,10),'yyyy/MM/dd/HH'))"
    val initFileTotal = 4
    new SparkJobExecute(sc).startJobForOnlineTdLog(dlSubject.get, files, matchKey, timeKey, partition, initFileTotal)
  }

  override def validate(sc: SQLContext, config: Config): SparkJobValidation = {
    var validation = true
    val error_msgs = ArrayBuffer[String]()

    Try(config.getString("path")) match {
      case Success(datalakeFiles) => this.dlFiles = Some(datalakeFiles)
      case Failure(ex) => {
        validation = false
        error_msgs += "not find input param datalakeFiles"
      }
    }

    Try(config.getString("subject")) match {
      case Success(datalakeSubject) => this.dlSubject = Some(datalakeSubject)
      case Failure(ex) => {
        validation = false
        error_msgs += "not find input param datalakeSubject"
      }
    }
    if (validation) SparkJobValid else SparkJobInvalid(error_msgs.mkString(";"))
  }
}

