package com.bestv.datalake.spark

/**
  * Package: com.bestv.datalake.spark. 
  * User: SichengWang 
  * Date: 2016/6/20 
  * Time: 11:11  
  * Project: DataLake
  */
object RestClientObject extends TRestClientInit{

  def addPartitionPath2Recorder(subject: String, partition: String, path: String) = partitionRecordAPI.createPartitionRecord(subject, partition, path) //增加子分区到记录表

  def deleteRecorderPath(subject: String, partition: String, path: String) = partitionRecordAPI.deletePartitionRecord(subject, partition, path) //删除记录表的分区

}
