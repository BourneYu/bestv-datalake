package com.bestv.datalake.common

/**
  * Package: com.bestv.datalake.spark. 
  * User: SichengWang 
  * Date: 2016/5/26 
  * Time: 12:35  
  * Project: DataLake
  */
object StatusEnum extends Enumeration{
  type FileStatus = Value
  val NEW = Value("new")
  val PENDING = Value("pending")
  val RUNNING = Value("running")
  val DONE = Value("done")
  val FAILED = Value("failed")
}
