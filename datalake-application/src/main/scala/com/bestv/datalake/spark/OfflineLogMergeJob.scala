package com.bestv.datalake.spark

import com.bestv.datalake.common.StatusEnum
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
class OfflineLogMergeJob extends TRestClientInit with SparkSqlJob {

  var datalakeSubjectPGIDs = ""
  var datalakeSubjectName = ""
  final val SUBJECTSTATUSDEFAULT = StatusEnum.PENDING.toString

  override def runJob(sc: SQLContext, jobConfig: Config): Any = {
    //    sc.hadoopConfiguration.set("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.DirectParquetOutputCommitter")
    val pgids = datalakeSubjectPGIDs.split("\\|", -1).toList
    val partition = super.getPartition(datalakeSubjectName, null)
    val initFileTotal = super.getFileTotal(datalakeSubjectName, null)
    val files = super.getOfflineFilesByPGIds(datalakeSubjectName, pgids)
    new SparkJobExecute(sc).startJobForOfflineLog(datalakeSubjectName, files, partition, initFileTotal)
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
