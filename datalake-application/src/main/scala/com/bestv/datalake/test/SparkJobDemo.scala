package com.bestv.datalake.test

import com.bestv.datalake.common.StatusEnum
import com.typesafe.config.Config
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

/**
  * Package: com.bestv.datalake.spark. 
  * User: SichengWang 
  * Date: 2016/6/14
  * Time: 15:35  
  * Project: DataLake
  */
class SparkJobDemo  extends SparkJob {

  var datalakeSubjectStatus = ""
  var datalakeSubjectName = ""
  final val SUBJECTSTATUSDEFAULT = StatusEnum.NEW.toString

  override def runJob(sc: SparkContext, jobConfig: Config): Any = {
    val sqlContext = new SQLContext(sc)
    val df  = sqlContext.read.parquet("s3://cloud-bigdata/datalake/IPTV_NANJING/IPTV_SQUIDLOG_VISPLAY_PARSER/2016/06/27/*/*/*.parquet")
    val tableName = "table"+System.currentTimeMillis().toString
    df.registerTempTable(tableName)
    val nums = sqlContext.sql(s"select updateTime from $tableName").distinct.collectAsList()
    sc.parallelize(nums).saveAsTextFile("s3://cloud-bigdata/test/datalake/result/"+tableName)
  }

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    var validation = true
    val error_msgs = ArrayBuffer[String]()

    Try(config.getString("input.datalakeSubjectStatus")) match {
      case Success(datalakeSubjectStatus) => this.datalakeSubjectStatus = datalakeSubjectStatus
      case Failure(ex) => {
        this.datalakeSubjectStatus = SUBJECTSTATUSDEFAULT
      }
    }

    Try(config.getString("input.datalakeSubject")) match {
      case Success(datalakeSubject) => this.datalakeSubjectName = datalakeSubject
      case Failure(ex) => {
        validation = false
        error_msgs += "not find input param datalakeSubject"
      }
    }
    if (validation) SparkJobValid else SparkJobInvalid(error_msgs.mkString(";"))
  }
}
