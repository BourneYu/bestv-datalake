package com.bestv.datalake.common

import java.io.IOException
import java.net.URISyntaxException
import java.util.UUID

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer

/**
  * Package: com.bestv.datalake.common. 
  * User: SichengWang 
  * Date: 2017/1/22 
  * Time: 11:18
  * Project: DataLake
  */
class FSOperation(fs: FileSystem) {

  /**
    * 获取指定files
 *
    * @param subject
    * @param actionTime
    * @return
    */
  def readFilesByPartition(subject: String, actionTime: String): List[String] = {
    val arraySub = splitSubject(subject)
    val strDir = FSOperation.PREFIX + "/" + arraySub(0) + "/" + arraySub(1) + "/"
    val dirPath = strDir + actionTime
    val fileStatuses = fs.listStatus(new Path(dirPath))
    val listFilePath = new ListBuffer[String]
    for (fileStatus <- fileStatuses) listFilePath += (fileStatus.getPath.toString)
    listFilePath.toList
  }

  /**
    * 判断subject分区是否存在
 *
    * @param subject
    * @param actionTime
    * @throws java.io.IOException
    * @throws java.net.URISyntaxException
    * @return
    */
  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def pathExitJudge(subject: String, actionTime: String): Boolean = {
    val arraySub = splitSubject(subject)
    val strDir = FSOperation.PREFIX + "/" + arraySub(0) + "/" + arraySub(1) + "/"
    val dirPath = strDir + actionTime
    pathExitJudge(dirPath)
  }

  /**
    * 判断path是否存在
 *
    * @param path
    * @throws java.io.IOException
    * @throws java.net.URISyntaxException
    * @return
    */
  @throws(classOf[IOException])
  @throws(classOf[URISyntaxException])
  def pathExitJudge(path: String): Boolean  = fs.exists(new Path(path))

  /**
    * 删除Path
 *
    * @param path
    * @return
    */
  def deletePath(path: String): Boolean =  fs.delete(new Path(path), true)


  /**
    * 存储DataFrame->DataLake
 *
    * @param subject
    * @param actionTime
    * @param dataFrame
    * @param count
    * @return
    */
  def saveDF2DataLakeParquet(subject: String, actionTime: String, dataFrame: DataFrame, count: Int): String = {
    val arraySub = splitSubject(subject)
    val strDir = FSOperation.PREFIX + "/" + arraySub(0) + "/" + arraySub(1) + "/" + actionTime + "/" + UUID.randomUUID
    dataFrame.coalesce(count).write.parquet(strDir)
    strDir
  }

  /**
    * 存储DataFrame->DataLakeBackup
 *
    * @param subject
    * @param actionTime
    * @param dataFrame
    * @param count
    * @return
    */
  def saveDF2BackupParquet(subject: String, actionTime: String, dataFrame: DataFrame, count: Int): String = {
    val arraySub = splitSubject(subject)
    val strDir = FSOperation.BACKUPPREFIX + "/" + arraySub(0) + "/" + arraySub(1) + "/" + actionTime + "/" + UUID.randomUUID
    dataFrame.coalesce(count).write.parquet(strDir)
    strDir
  }

  /**
    * 切分主题
 *
    * @param subject
    * @return
    */
  def splitSubject(subject: String): Array[String] = {
    val arraySub = subject.split(FSOperation.SPLITSIGN, -1)
    if(arraySub.length < 2) throw new IllegalArgumentException("subject format is error!")
    arraySub
  }

}

object FSOperation{
  val PREFIX = "s3://cloud-bigdata/datalake"//datalake路径头
  val BACKUPPREFIX = "s3://cloud-bigdata/datalakebackup"//datalakeBackup路径头
  val SUFFIX = ".parquet"
  val SPLITSIGN = "\\."
}
