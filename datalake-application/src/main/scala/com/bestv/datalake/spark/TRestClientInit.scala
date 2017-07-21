package com.bestv.datalake.spark

import java.util.concurrent.ConcurrentHashMap

import io.swagger.client.apiImpl._
import io.swagger.client.model.{PartitionRecord, VPartitionRecord}

import scala.collection.JavaConversions._
import scala.collection.mutable


/**
  * Package: com.bestv.datalake.spark. 
  * User: SichengWang 
  * Date: 2016/6/1 
  * Time: 14:22  
  * Project: DataLake
  */
trait TRestClientInit {

  val logRulerAPI = new LogrulercontrollerImplApi()

  val pgOnlineAPI = new PathgroupcontrollerImplApi()

  val pgcOfflineAPI = new PathgroupcontrollerofflineImplApi()

  val partitionRecordAPI = new PartitionrecordcontrollerImplApi()

  val partitionRulerAPI = new PartitionrulercontrollerImplApi()

  val subjectImplAPI = new SubjectcontrollerImplApi()

  def getMatchKey(subject: String): String = logRulerAPI.getLogRules(subject).get(0).getMatchKey

  def getFileType(subject:String):String = subjectImplAPI.getSubjectByName(subject).getFileType

  def getTimeKey(subject: String): String = logRulerAPI.getLogRules(subject).get(0).getTimeKey

  def getPartition(subject: String, status: String): String = partitionRulerAPI.getPartitionRule1(subject, status).get(0).getPartitionrule

  def getFileTotal(subject: String, status: String): Int = partitionRulerAPI.getPartitionRule1(subject, status).get(0).getInitfiletotal

//  def getOnlineFiles(subject: String, status: String): List[String] = pgOnlineAPI.getFilesByPGS(pgOnlineAPI.getPathGroups(subject,null,status)).toList

  def getOnlineFilesByPGIds(subject: String, pgIds: List[String]): List[String] = pgOnlineAPI.getFilesByPGIds(subject,pgIds).toList

//  def getOfflineFiles(subject: String, status: String): List[String] = pgcOfflineAPI.getFilesByOPGS(pgcOfflineAPI.getPathGroupOfflines(subject, status,null)).toList

  def getOfflineFilesByPGIds(subject: String, pgIds: List[String]): List[String] = pgcOfflineAPI.getFilesByOPGIds(subject,pgIds).toList

  def getHistoryPartition2Paths(subject: String): ConcurrentHashMap[String, List[String]] = {
    val partition2Paths = mutable.Map[String, List[String]]()
    partitionRecordAPI.getCompressPartitionBySubject(subject).foreach { case (partition, paths) => partition2Paths += (partition -> paths.toList) }
    new ConcurrentHashMap[String, List[String]](partition2Paths)
  }

  def getPartitionsByPartitionName(subject: String,partitionName:String):List[PartitionRecord] = partitionRecordAPI.getPartitionRecords(subject,partitionName).toList

  def getPartitionByPartitionId(partitionId:String):List[VPartitionRecord] = partitionRecordAPI.getVpartitionRecordUsingGET(partitionId).toList

}


