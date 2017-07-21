package com.bestv.datalake.spark

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar

import com.bestv.datalake.common.{CustomMapWritable, JsonInputFormat, FSOperation, SchemaUtil}
import com.bestv.datalake.hive.MetaManager
import org.apache.commons.lang.time.DateFormatUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{Text, LongWritable}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
  * Package: com.bestv.bdp.spark
  * User: SichengWang 
  * Date: 2016/7/12 
  * Time: 14:19  
  * Project: DataLake
  */
class SparkJobExecute(sqlContext: SQLContext) extends Serializable {

  @transient val conf = new Configuration
  val fileSystem = FileSystem.newInstance(new URI("s3://cloud-bigdata/"), conf)
  val fsOperation = new FSOperation(fileSystem)
  val metaManager = new MetaManager

  /**
    * 在线日志merge
    *
    * @param subject   处理日志类型
    * @param files     待处理日志路径
    * @param matchKey  参与去重字段名
    * @param timeKey   时间字段标识
    * @param partition 分区类型
    */
  def startJobForOnlineAvroLog(subject: String, files: List[String], matchKey: String, timeKey: String, partition: String, initFileTotal: Int) = {
    val originalDataFrame = loadAvroFile(files)
    dfMergeFunction(subject, originalDataFrame, matchKey, timeKey, partition, initFileTotal)
  }



  /**
    * 在线CsvLog日志merge
    *
    * @param subject   处理日志类型
    * @param files     待处理日志路径
    * @param matchKey  参与去重字段名
    * @param timeKey   时间字段标识
    * @param partition 分区类型
    */
  def startJobForOnlineCsvLog(subject: String, files: List[String], matchKey: String, timeKey: String, partition: String, initFileTotal: Int) = {
    val originalDataFrame = loadCsvFile(files)
    val schemaPipeLine = SchemaUtil.getPipeLineSchema(subject)
    val matchFields = matchDF2Schema(originalDataFrame.schema,schemaPipeLine)
    val newDataFrame = resetDataFrame(originalDataFrame,matchFields)
    dfMergeNewFunction(subject, newDataFrame, matchKey, timeKey, partition, initFileTotal)
  }


  /**
    * 在线JsonLog日志merge
    *
    * @param subject   处理日志类型
    * @param files     待处理日志路径
    * @param matchKey  参与去重字段名
    * @param timeKey   时间字段标识
    * @param partition 分区类型
    */
  def startJobForOnlineJsonLog(subject: String, files: List[String], matchKey: String, timeKey: String, partition: String, initFileTotal: Int) = {
    val originalDataFrame = loadJsonFile(files)
    val schemaPipeLine = SchemaUtil.getPipeLineSchema(subject)
    val matchFields = matchDF2Schema(originalDataFrame.schema,schemaPipeLine)
    val newDataFrame = resetDataFrame(originalDataFrame,matchFields)
    dfMergeNewFunction(subject, newDataFrame, matchKey, timeKey, partition, initFileTotal)
  }

/**
  * def startJobForOnlineGbkJson(subject: String, files: List[String], matchKey: String, timeKey: String, partition: String, initFileTotal: Int)={
  * val originalDataFrame = loadGbkJsonFile(files)
  * val schemaPipeLine = SchemaUtil.getPipeLineSchema(subject)
  * val matchFields = matchDF2Schema(originalDataFrame.schema,schemaPipeLine)
  * val newDataFrame = resetDataFrame(originalDataFrame,matchFields)
  * dfMergeNewFunction(subject, newDataFrame, matchKey, timeKey, partition, initFileTotal)
  * }**/


  /**
    * ocj gbk json
    *
    * @param subject
    * @param files
    * @param matchKey
    * @param timeKey
    * @param partition
    * @param initFileTotal
    */
  def startJobForOnlineGbkJson(subject: String, files: List[String], matchKey: String, timeKey: String, partition: String, initFileTotal: Int)={
    val sc = sqlContext.sparkContext
    val rddTotal = sc.union(for(aFile:String <- files) yield sc.newAPIHadoopFile(aFile,classOf[JsonInputFormat], classOf[LongWritable], classOf[CustomMapWritable], conf))
    val schemaPipeLine = SchemaUtil.getPipeLineSchema(subject) //获取json Schema
    val fields = schemaPipeLine.fieldNames
    val rddFinal = rddTotal.map(line =>{
      val values = for(aField:String <- fields) yield line._2.get(new Text(aField)).toString
      Row.fromSeq(values)})
    val createDF = sqlContext.createDataFrame(rddFinal, schemaPipeLine)
    dfMergeNewFunction(subject, createDF, matchKey, timeKey, partition, initFileTotal)
  }

  /**
    * 在线TDLog日志merge
    *
    * @param subject   处理日志类型
    * @param files     待处理日志路径
    * @param matchKey  参与去重字段名
    * @param timeKey   时间字段标识
    * @param partition 分区类型
    */
  def startJobForOnlineTdLog(subject: String, files: List[String], matchKey: String, timeKey: String, partition: String, initFileTotal: Int) = {
    import com.bestv.bdp.tdlog.schema.TD_EVENTPACKAGE
    import com.bestv.bdp.tdlog.spark.TdSparkContext._
    import com.bestv.bdp.tdlog.toolbox.EventPackageParser
    import com.databricks.spark.avro.AvroEncoder
    val sc = sqlContext.sparkContext

    implicit val sqlContextLocal = sc

    val encoder = AvroEncoder.of[TD_EVENTPACKAGE](TD_EVENTPACKAGE.getClassSchema)// 构造TD_EVENTPACKAGE Encoder
    val rddList = for(aFile:String <- files) yield sc.tdParagraphFile(aFile, 8)
    val rddTotal = sc.union(rddList)
    val rddParse = rddTotal.flatMap(log => {Seq(EventPackageParser.parse(log))})
    val input_df = sqlContext.createDataset(rddParse)(encoder).toDF
    dfMergeNewFunction(subject, input_df, matchKey, timeKey, partition, initFileTotal)
  }


  /**
    * 在线日志merge
    *
    * @param subject   处理日志类型
    * @param files     待处理日志路径
    * @param matchKey  参与去重字段名
    * @param timeKey   时间字段标识
    * @param partition 分区类型
    */
  def startJobForOnlineTxtLog(subject: String, files: List[String], matchKey: String, timeKey: String, partition: String, initFileTotal: Int) = {
    val originalDataFrame = loadTextFile(files)
    val rowRDD = originalDataFrame.rdd.map(line => Row.fromSeq(line.split("\\|")))
    val schemaPipeLine = SchemaUtil.getPipeLineSchema(subject) //获取text文件头
    val createDF = sqlContext.createDataFrame(rowRDD, schemaPipeLine)
    dfMergeNewFunction(subject, createDF, matchKey, timeKey, partition, initFileTotal)
  }



  /**
    * 离线日志merge
    *
    * @param subject
    * @param files
    * @param partition
    */
  @deprecated
  def startJobForOfflineLog(subject: String, files: List[String], partition: String, initFileTotal: Int) = {
    val backupDF = loadParquetFile(files) //加载backup Parquet Log
    val actionTimeFormatRows = backupDF.select(SparkJobExecute.ACTIONTIME).distinct.collectAsList.asScala //ActionTime时间集合
    val finalActionTimeSet = for (aActionTime: Row <- actionTimeFormatRows) yield aActionTime.getString(0)
    finalActionTimeSet.foreach(actionTime => {
      //加载在线日志属于属于actionTimeFormat的parquet数据
      val offlineMergeDF = getLogByActionTimeDF(backupDF, SparkJobExecute.ACTIONTIME, actionTime)
      val offlineMergeDFTableName = randomTableName(subject) //原始数据临时表
      registerDFTable(offlineMergeDFTableName, offlineMergeDF.drop(SparkJobExecute.ACTIONTIME))
      val distinctDF = distinctByMatchValue(offlineMergeDFTableName, SparkJobExecute.MATCHVALUE)
      sqlContext.dropTempTable(offlineMergeDFTableName) //清除临时表
      val distinctTableName = randomTableName(subject) //backup中特定小时临时表名
      registerDFTable(distinctTableName, distinctDF) //注册临时表
      fsOperation.pathExitJudge(subject, actionTime) match {
        case true =>
          //加载当前merge的parquet数据
          val needMergeMatchValueDF = loadParquetFile(fsOperation.readFilesByPartition(subject, actionTime)).select(SparkJobExecute.MATCHVALUE) //加载parquet matchValue
        val historyDatalakeTableName = randomTableName(subject) //数据湖中特定小时的分区表
          registerDFTable(historyDatalakeTableName, needMergeMatchValueDF) //注册临时表
        //日志去重
        val finalDataFrame = mergeTableByMatchValue(distinctTableName, historyDatalakeTableName, SparkJobExecute.MATCHVALUE)
          dropContextTempTable(historyDatalakeTableName) //清除临时表
        val inputPath = fsOperation.saveDF2DataLakeParquet(subject, actionTime, finalDataFrame, initFileTotal)
          addPatitionOperation(subject, actionTime, inputPath)
        case false =>
          val inputPath = fsOperation.saveDF2DataLakeParquet(subject, actionTime, distinctDF, initFileTotal)
          addPatitionOperation(subject, actionTime, inputPath)
      }
      dropContextTempTable(distinctTableName) //清除临时表
    })
  }

  /**
    * 历史日志压缩
    *
    * @param subject
    * @param partition
    * @param primitPaths
    * @param initFileTotal
    * @return
    */
  def startJobForHistoryLog(subject: String, partition: String, primitPaths: List[String], initFileTotal: Int) = {
    val finalPaths = new ListBuffer[String]()
    val partition2value = SparkJobExecute.PARTITIONPATTERN zip partition.split("/", -1)
    val instancePartition = partition2value.map(str => str._1 + "=" + str._2).mkString("/")
    //pathJudge
    for(i <- 0 until primitPaths.length){
      val path = primitPaths(i)
      if(fsOperation.pathExitJudge(path)) {finalPaths += path.concat("/*")}
      else{
        val instance = instancePartition + "/" + SparkJobExecute.RANDOMPATH + "=" + path.split("/", -1).last
        metaManager.dropPartition(subject, instance) //hive删除分区
        RestClientObject.deleteRecorderPath(subject, partition, path) //记录表删除分区
      }
    }
    val newPath = fsOperation.saveDF2DataLakeParquet(subject, partition, loadParquetFile(finalPaths.toList), initFileTotal)
    deletePatitionOperation(subject, partition, primitPaths) //删除分区系列操作
    addPatitionOperation(subject, partition, newPath) //添加分区系列操作
  }

  /**
    * 格式化dataFrame处理Function
    *
    * @param subject
    * @param originalDataFrame
    * @param matchKey
    * @param timeKey
    * @param partition
    * @param initFileTotal
    */
  def dfMergeFunction(subject: String, originalDataFrame: DataFrame, matchKey: String, timeKey: String, partition: String, initFileTotal: Int) = {
    val createDFTableName = randomTableName(subject) //原始数据临时表
    registerDFTable(createDFTableName, originalDataFrame)
    val expansionDataFrame = dataFrameExpansion(createDFTableName, matchKey, timeKey) //日志拉伸
    dropContextTempTable(createDFTableName) //清除临时表
    val actionTimeRows = expansionDataFrame.select(SparkJobExecute.ACTIONTIME).distinct.collectAsList.asScala
    val actionTimeSet = for (aActionTime: Row <- actionTimeRows) yield aActionTime.getString(0)
    actionTimeSet.foreach(actionTime => {
      //加载在线日志属于属于actionTime的数据
      val correntMergeDF = getLogByActionTimeDF(expansionDataFrame, SparkJobExecute.ACTIONTIME, actionTime)
      val correntMergeDFTableName = randomTableName(subject) //原始数据临时表
      registerDFTable(correntMergeDFTableName, correntMergeDF.drop(SparkJobExecute.ACTIONTIME))
      val distinctDF = distinctByMatchValue(correntMergeDFTableName, SparkJobExecute.MATCHVALUE)
      sqlContext.dropTempTable(correntMergeDFTableName) //清除临时表
      val distinctTableName = randomTableName(subject) //S3中特定分区表
      registerDFTable(distinctTableName, distinctDF)
      fsOperation.pathExitJudge(subject, actionTime) match {
        case true => //DL已存在此分区
          val anotherMergeMatchValueLog = loadParquetFile(fsOperation.readFilesByPartition(subject, actionTime)).select(SparkJobExecute.MATCHVALUE) //加载parquet matchValue
          val anotherMergeTableName = randomTableName(subject)
          registerDFTable(anotherMergeTableName, anotherMergeMatchValueLog) //加载数据湖数据
          val finalDataFrame = mergeTableByMatchValue(distinctTableName, anotherMergeTableName, SparkJobExecute.MATCHVALUE) //日志去重
          dropContextTempTable(anotherMergeTableName) //清除临时表
          val inputPath = fsOperation.saveDF2DataLakeParquet(subject, actionTime, finalDataFrame, initFileTotal) //存储数据
          addPatitionOperation(subject, actionTime, inputPath)
          case false => //DL不存在此分区
          val inputPath = fsOperation.saveDF2DataLakeParquet(subject, actionTime, distinctDF, initFileTotal)
          addPatitionOperation(subject, actionTime, inputPath)
      }
      dropContextTempTable(distinctTableName) //清除临时表
    })
  }


  /**
    * 格式化dataFrame处理Function 最新
    * updataTime,matchValue add to header
    *
    * @param subject
    * @param originalDataFrame
    * @param matchKey
    * @param timeKey
    * @param partition
    * @param initFileTotal
    */
  def dfMergeNewFunction(subject: String, originalDataFrame: DataFrame, matchKey: String, timeKey: String, partition: String, initFileTotal: Int) = {
    val createDFTableName = randomTableName(subject) //原始数据临时表
    registerDFTable(createDFTableName, originalDataFrame)
    val expansionDataFrame = dataFrameNewExpansion(createDFTableName, matchKey, timeKey) //日志拉伸
    dropContextTempTable(createDFTableName) //清除临时表
    val actionTimeRows = expansionDataFrame.select(SparkJobExecute.ACTIONTIME).distinct.collectAsList.asScala
    val actionTimeSet = for (aActionTime: Row <- actionTimeRows) yield aActionTime.getString(0)
    actionTimeSet.foreach(actionTime => {
      //加载在线日志属于属于actionTime的数据
      val correntMergeDF = getLogByActionTimeDF(expansionDataFrame, SparkJobExecute.ACTIONTIME, actionTime)
      val correntMergeDFTableName = randomTableName(subject) //原始数据临时表
      registerDFTable(correntMergeDFTableName, correntMergeDF.drop(SparkJobExecute.ACTIONTIME))
      val distinctDF = distinctByMatchValue(correntMergeDFTableName, SparkJobExecute.MATCHVALUENEW)
      sqlContext.dropTempTable(correntMergeDFTableName) //清除临时表
      val distinctTableName = randomTableName(subject) //S3中特定分区表
      registerDFTable(distinctTableName, distinctDF)
      fsOperation.pathExitJudge(subject, actionTime) match {
        case true => //DL已存在此分区
          val anotherMergeMatchValueLog = loadParquetFile(fsOperation.readFilesByPartition(subject, actionTime)).select(SparkJobExecute.HEADER) //加载parquet matchValue
          val anotherMergeTableName = randomTableName(subject)
          registerDFTable(anotherMergeTableName, anotherMergeMatchValueLog) //加载数据湖数据
          val finalDataFrame = mergeTableByMatchValue(distinctTableName, anotherMergeTableName, SparkJobExecute.MATCHVALUENEW) //日志去重
          dropContextTempTable(anotherMergeTableName) //清除临时表
          val inputPath = fsOperation.saveDF2DataLakeParquet(subject, actionTime, finalDataFrame, initFileTotal) //存储数据
          addPatitionOperation(subject, actionTime, inputPath)
        case false => //DL不存在此分区
          val inputPath = fsOperation.saveDF2DataLakeParquet(subject, actionTime, distinctDF, initFileTotal)
          addPatitionOperation(subject, actionTime, inputPath)
      }
      dropContextTempTable(distinctTableName) //清除临时表
    })
  }



  /**
    *
    * 删除分区操作
    *
    * @param subject
    * @param actionTime
    * @param paths
    * @return
    */
  def deletePatitionOperation(subject: String, actionTime: String, paths: List[String]) = {
    val partition2value = SparkJobExecute.PARTITIONPATTERN zip actionTime.split("/", -1)
    val instancePartition = partition2value.map(str => str._1 + "=" + str._2).mkString("/")
    paths.foreach(path => {
      val instance = instancePartition + "/" + SparkJobExecute.RANDOMPATH + "=" + path.split("/", -1).last
      metaManager.dropPartition(subject, instance) //hive删除分区
      RestClientObject.deleteRecorderPath(subject, actionTime, path) //记录表删除分区
      fsOperation.deletePath(path) //S3删除文件
    })
  }

  /**
    * 添加分区操作
    *
    * @param subject
    * @param partition
    * @param location
    * @return
    */
  def addPatitionOperation(subject: String, partition: String, location: String) = {
    metaManager.addPartition(subject, partition + "/" + location.split("/", -1).last, location) //hive添加
    RestClientObject.addPartitionPath2Recorder(subject, partition, location) //记录表添加分区}
  }

  /**
    * 删除临时表
    *
    * @param tableName
    * @return
    */
  def dropContextTempTable(tableName: String): Boolean = {
    try {
      sqlContext.dropTempTable(tableName)
    } catch {
      case ex: NoSuchTableException => return false
    }
    true
  }

  /**
    * 数据Schema与repo schema配对
    *
    * @param dataSchema
    * @param pipeLineSchema
    * @return
    */
  def matchDF2Schema(dataSchema: StructType, pipeLineSchema: StructType):Seq[String] = pipeLineSchema.fieldNames.map(aName => {
    if (dataSchema.fieldNames.contains(aName)) aName
    else "null as "+aName
  }).toSeq


  /**
    * DataFrame 扩充
    *
    * @param tableName
    * @param matchKey
    * @return
    */
  def dataFrameExpansion(tableName: String, matchKey: String, timeKey: String): DataFrame = {
    val expansionSql = s"SELECT * ,unix_timestamp() datalakeUpdateTime,$matchKey datalakeMatchValue,$timeKey datalakeActionTime FROM $tableName"
    sqlContext.sql(expansionSql)
  }


  /**
    * DataFrame 扩充
    *
    * @param tableName
    * @param matchKey
    * @return
    */
  def dataFrameNewExpansion(tableName: String, matchKey: String, timeKey: String): DataFrame = {
    val expansionSql = s"SELECT named_struct('datalakeUpdateTime' ,unix_timestamp(),'datalakeMatchValue', $matchKey) as header , $timeKey datalakeActionTime , * FROM $tableName"
    sqlContext.sql(expansionSql)
  }

  /**
    * tableA tableB leftJoin
    *
    * @param tableA     A表
    * @param tableB     B表
    * @param matchName A参与join的字段名
    * @return A中出现且未在B中出现的数据
    */
  def mergeTableByMatchValue(tableA: String, tableB: String, matchName: String): DataFrame = {
    val mergeDFSql = s"SELECT A.* FROM $tableA A LEFT JOIN $tableB B on A.$matchName = B.$matchName WHERE B.$matchName IS NULL"
    sqlContext.sql(mergeDFSql)
  }

  /**
    * table 根据字段去重
    *
    * @param table     表名
    * @param matchName 去重字段名
    * @return dataFrame
    */
  def distinctByMatchValue(table: String, matchName: String): DataFrame = {
    val distinctSql = s"SELECT * FROM (SELECT ROW_NUMBER() OVER(PARTITION BY $matchName ORDER BY floor(RAND() * 100)) AS RowId,* FROM $table) x WHERE x.RowId=1"
    sqlContext.sql(distinctSql).drop("RowId")
  }


  /**
    * table 根据字段去重
    *
    * @param table     表名
    * @param matchName 去重字段名
    * @return dataFrame
    */
  def distinctByMatchValueOrderByTime(table: String, matchName: String, orderbyName: String): DataFrame = {
    val distinctSql = s"SELECT * FROM (SELECT ROW_NUMBER() OVER(PARTITION BY $matchName ORDER BY $orderbyName DESC) AS RowId,* FROM $table) x WHERE x.RowId=1"
    sqlContext.sql(distinctSql).drop("RowId")
  }


  /**
    * 根据actionTime加载数据
    *
    * @param tableName  表名
    * @param col        列名
    * @param actionTime 日志时间
    * @return
    */
  def getLogByActionTime(tableName: String, col: String, actionTime: String): DataFrame = {
    val selectLogByActionTime = s"select * from $tableName where $col = '$actionTime'"
    sqlContext.sql(selectLogByActionTime)
  }

  /**
    * DataFrame Filter
    *
    * @param df
    * @param col
    * @param actionTime
    * @return
    */
  def getLogByActionTimeDF(df: DataFrame, col: String, actionTime: String): DataFrame = df.filter(s"$col = '$actionTime'")


  /**
    * loadParquetFile
    *
    * @param files 文件路径
    * @return
    */
  def loadParquetFile(files: List[String]): DataFrame = sqlContext.read.load(files: _*)


  /**
    * loadAvroFile
    *
    * @param files
    * @return
    */
  def loadAvroFile(files: List[String]): DataFrame = sqlContext.read.format(SparkJobExecute.SOURCESPARKAVROPACK).load(files: _*)

  /**
    * loadTextFile
    *
    * @param files
    * @return
    */
  def loadTextFile(files: List[String]): Dataset[String] = sqlContext.read.textFile(files: _*)

  /**
    * loadCsvFile
    *
    * @param files
    * @return
    */
  def loadCsvFile(files: List[String]): DataFrame = sqlContext.read.option("header", "true").csv(files: _*)

  /**
    * loadJsonFile
    *
    * @param files
    * @return
    */
  def loadJsonFile(files: List[String]): DataFrame = sqlContext.read.option("header", "true").json(files: _*)


  /**
    * loadGbkJsonFile
    *
    * @param files
    * @return
    */
  def loadGbkJsonFile(files: List[String]): DataFrame = sqlContext.read.format("org.apache.spark.sql.execution.datasources.gbkjson").load(files: _*)

  /**
    * 生成随机表名
    *
    * @param subject
    * @return
    */
  def randomTableName(subject: String): String = subject.replace(".", "_") + System.currentTimeMillis

  /**
    * 临时表注册
    *
    * @param tableName
    * @param dataFrame
    */
  def registerDFTable(tableName: String, dataFrame: DataFrame) = dataFrame.registerTempTable(tableName)


  /**
    * 当前时间formatByPartition
    * timeMillis 绝对毫秒数
    * partition  分区格式 yyyy/MM/dd/HH/mm/ss
    *
    * @return "2016/05/25/15"
    */
  def getDateFormatByPartition(timeMillis: Long, partition: String): String = DateFormatUtils.format(timeMillis, partition)

  /**
    * DataFrame字段重排序
    *
    * @param matchFields
    * @return
    */
  def resetDataFrame(dataFrame: DataFrame, matchFields: Seq[String]) : DataFrame = dataFrame.selectExpr(matchFields:_*)


  /**
    * 时间比较
    *
    * @param actionTime  yyyy/MM/dd/HH/mm/ss
    * @param currentTime yyyy/MM/dd/HH/mm/ss
    * @return true:<=intervalTime false:>intervalTime
    */
  @deprecated
  def compareActionTime(actionTime: String, currentTime: String): Boolean = {
    val rules = actionTime.split("/").length match {
      case 1 => "yyyy"
      case 2 => "yyyy/MM"
      case 3 => "yyyy/MM/dd"
      case 4 => "yyyy/MM/dd/HH"
      case 5 => "yyyy/MM/dd/HH/mm"
      case 6 => "yyyy/MM/dd/HH/mm/ss"
    }

    val intervalTimes: Long = actionTime.split("/").length match {
      case 1 => 3 * 365 * 3600 * 1000 * 24 //3 year
      case 2 => 3 * 31 * 3600 * 1000 * 24 //3 month
      case 3 => 31 * 3600 * 1000 * 24 //31 day
      case 4 => 7 * 3600 * 1000 * 24 //7 day
      case _ => 3 * 365 * 3600 * 1000 * 24
    }

    val diff: Long = stringDateToTimeMillis(currentTime, rules) - stringDateToTimeMillis(actionTime, rules)
    true
  }

  /**
    * 获取绝对秒数
    *
    * @param stringTime
    * @param rule
    * @return
    */
  def stringDateToTimeMillis(stringTime: String, rule: String): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTime(new SimpleDateFormat(rule).parse(stringTime))
    calendar.getTimeInMillis()
  }

}

object SparkJobExecute extends Serializable {
  final val UPDATETIME = "datalakeUpdateTime"
  final val HEADER = "header"
  final val UPDATETIMENEW = "header.datalakeUpdateTime"
  final val MATCHVALUE = "datalakeMatchValue"
  final val MATCHVALUENEW = "header.datalakeMatchValue"
  final val ACTIONTIME = "datalakeActionTime"
  final val SOURCESPARKAVROPACK = "com.databricks.spark.avro"
  final val PARTITIONPATTERN = List("year", "month", "day", "hour")
  final val RANDOMPATH = "randompath"
}

