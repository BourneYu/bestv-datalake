package com.bestv.datalake.hive

import java.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.bestv.bdp.schema.SchemaMetadata
import com.bestv.bdp.schema.repository.{BaseSchemaRepository, SchemaRepositoryFactory, SchemaRepositoryType}
import io.swagger.client.apiImpl.PartitionrulercontrollerImplApi
import io.swagger.client.model.PartitionRuler
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.hadoop.hive.metastore.api
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Client
import org.apache.hadoop.hive.metastore.api.{FieldSchema, _}
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket

import scala.collection.JavaConversions._

/**
  * Created by chen.qianjin on 2016/8/8.
  */
class MetaManager {
  val transport = new TSocket("10.200.8.157", 9083)
  val protocol = new TBinaryProtocol(transport)
  val client: Client = new ThriftHiveMetastore.Client(protocol)
  val db_suffix = "datalake_"

  /**
    * 添加分区
    *
    * @param subject
    * @param instancePartition
    * @param location
    */
  def addPartition(subject: String, instancePartition: String, location: String): Boolean = {
    val partition: Partition = new Partition()
    partition.setDbName(db_suffix + subject.split("\\.")(0)) //subject="iptv_nanjing.iptv_squidlog_loguni_parser"
    partition.setTableName(subject.split("\\.")(1))
    val valueList: util.ArrayList[String] = new util.ArrayList[String]()
    val partSplits: Array[String] = instancePartition.split("/") //instancePartition="2016/08/09/09/xxxxxxxxx"
    for (ps <- partSplits) {
      valueList.add(ps)
    }
    partition.setValues(valueList)
    val sd: StorageDescriptor = new StorageDescriptor()
    sd.setLocation(location)
    val serDeInfo: SerDeInfo = new SerDeInfo()
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
    sd.setSerdeInfo(serDeInfo)
    sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
    partition.setSd(sd)
    try {
      transport.open()
      client.add_partition(partition)
      true
    } catch {
      case e: Exception => false
    } finally {
      transport.close()
    }
  }

  /**
    * 根据主题和具体分区名称删除分区
    *
    * @param subject
    * @param partName
    * @return
    */
  def dropPartition(subject: String, partName: String): Boolean = {
    val dbName = db_suffix + subject.split("\\.")(0)
    val tableName = subject.split("\\.")(1)
    try {
      transport.open()
      client.drop_partition_by_name(dbName, tableName, partName, true)
    } catch {
      case e: Exception => e.printStackTrace
        false
    } finally {
      transport.close()
    }
  }

  /**
    * 删除表
    *
    * @param subject
    */
  def dropTable(subject: String): Boolean = {
    val dbName = db_suffix + subject.split("\\.")(0)
    val tableName = subject.split("\\.")(1)
    try {
      transport.open()
      client.drop_table(dbName, tableName, true)
      true
    } catch {
      case e: Exception => false
    } finally {
      transport.close()
    }
  }

  /**
    * 根据subject查询最新的schema
    *
    * @param subject
    * @return
    */
  def getLatestSchemaBySubject(subject: String): Schema = {
    //连接远程的schema repo
    //val baseSchemaRepository: BaseSchemaRepository = SchemaRepositoryFactory.initSchemaRepository("test", "http://platform.bestv.com.cn:8089", SchemaRepositoryType.REMOTE)
    //测试的schema registry地址
    val baseSchemaRepository: BaseSchemaRepository = SchemaRepositoryFactory.initSchemaRepository("test", "http://10.200.8.35:8081", SchemaRepositoryType.REMOTE)
    //根据主题获取最新的schema
    val latestSchemaMetadata: SchemaMetadata = baseSchemaRepository.getLatestSchemaMetadata(subject)
    val schema: Schema = latestSchemaMetadata.getSchema
    schema
  }

  /**
    * 创建hive表
    *
    * @param subject
    */
  def createTable(subject: String): Boolean = {
    val table: Table = new Table()
    table.setDbName(db_suffix + subject.split("\\.")(0))
    table.setTableName(subject.split("\\.")(1))
    table.setParameters(Map("EXTERNAL" -> "TRUE"))
    val sd: StorageDescriptor = new StorageDescriptor()
    val cols: util.List[FieldSchema] = convertToFieldSchemas(subject)
    sd.setCols(cols)
    val serDeInfo: SerDeInfo = new api.SerDeInfo()
    serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
    sd.setSerdeInfo(serDeInfo)
    sd.setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
    sd.setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
    table.setSd(sd)
    val partitionKeys: util.List[FieldSchema] = converToPartitionKeys(subject)
    table.setPartitionKeys(partitionKeys)
    try {
      transport.open()
      client.create_table(table)
      true
    } catch {
      case e: Exception => false
    } finally {
      transport.close()
    }
  }
  /**
    * schema的字段转换为hive的FieldSchema
    *
    * @param subject
    * @return
    */
  def convertToFieldSchemas(subject: String): util.List[FieldSchema] = {
    val schemaMetadata = getLatestSchemaBySubject(subject)
    val fields: util.List[Field] = schemaMetadata.getFields
    val cols: util.ArrayList[FieldSchema] = new util.ArrayList[FieldSchema]()
    for (f <- fields) {
      val fs: FieldSchema = new api.FieldSchema()
      fs.setName(f.name)
      val schemaStr: String = f.schema().toString()
      if (!f.name.equals("header")) {
        val splitStrs: Array[String] = schemaStr.substring(1, schemaStr.length - 1).replace("\"", "").split(",")
        for (s <- splitStrs) {
          s.equals("null") match {
            case true =>
            case false => fs.setType(s)
          }
        }
      } else {
        val parseObject: JSONObject = JSON.parseObject(schemaStr)
        val parseArray: JSONArray = JSON.parseArray(parseObject.get("fields").toString)
        var headerType = "struct<"
        for (e <- parseArray) {
          val obj: JSONObject = JSON.parseObject(e.toString)
          val typeValue = obj.get("type").toString
          headerType += obj.get("name")
          if (typeValue.contains(",")) {
            typeValue.contains("fixed") match {
              case true => {
                headerType += ":string,"
              }
              case false => {
                val splitArr: Array[String] = typeValue.substring(1, typeValue.length - 1).replace("\"", "").split(",")
                for (sp <- splitArr) {
                  sp.equals("null") match {
                    case true =>
                    case false => headerType += ":" + sp + ","
                  }
                }
              }
            }
          } else {
            typeValue.equals("long") match {
              case true => headerType += ":bigint,"
              case false => headerType += ":" + typeValue + ","
            }
          }
        }
        headerType = headerType.substring(0, headerType.length - 1) + ">"
        fs.setType(headerType)
      }
      cols.add(fs)
    }
    cols
  }

  /**
    * schema中的分区字段转换为hive的FieldSchema
    *
    * @param subject
    * @return
    */
  def converToPartitionKeys(subject: String): util.List[FieldSchema] = {
    //调用分区服务获取主题表分区规则信息
    val implApi: PartitionrulercontrollerImplApi = new PartitionrulercontrollerImplApi()
    val rules: util.List[PartitionRuler] = implApi.getPartitionRule1(subject, null)
    val partitionKeys: util.ArrayList[FieldSchema] = new util.ArrayList[FieldSchema]()
    if (rules != null && rules.size() > 0) {
      val partitionRule: String = rules.get(0).getPartitionrule
      var newRules = Array[String]("")
      if (partitionRule != null && partitionRule.length > 0) {
        val newPartitionRule = partitionRule match {
          case "yyyy" => "year"
          case "yyyy/MM" => "year/month"
          case "yyyy/MM/dd" => "year/month/day"
          case "yyyy/MM/dd/HH" => "year/month/day/hour"
        }
        newRules = newPartitionRule.split("/")
        for (i <- newRules) {
          val partKey: FieldSchema = new api.FieldSchema()
          partKey.setName(i)
          partKey.setType("string")
          partitionKeys.add(partKey)
        }
        val randomPartKey: FieldSchema = new FieldSchema()
        randomPartKey.setName("randompath")
        randomPartKey.setType("string")
        partitionKeys.add(randomPartKey)
      }
    }
    partitionKeys
  }
}