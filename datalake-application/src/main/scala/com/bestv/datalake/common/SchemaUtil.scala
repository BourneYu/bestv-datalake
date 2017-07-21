package com.bestv.datalake.common

import com.bestv.bdp.schema.repository.{SchemaRepositoryType, SchemaRepositoryFactory}
import com.bestv.datalake.avro.SchemaConverters
import org.apache.spark.sql.types.StructType

/**
  * Package: com.bestv.datalake.common. 
  * User: SichengWang 
  * Date: 2017/6/30 
  * Time: 15:55  
  * Project: DataLake
  */
object SchemaUtil {

  /**
    * 获取一级schema
    * @param subject
    * @return
    */
  def getPipeLineSchema(subject:String):StructType = {
    val baseSchemaRepository = SchemaRepositoryFactory.initSchemaRepository("test000","http://124.108.10.57:8089", SchemaRepositoryType.REMOTE)
    val schemaMetadata = baseSchemaRepository.getLatestSchemaMetadata(subject)
    SchemaConverters.toSqlType(schemaMetadata.getSchema).dataType.asInstanceOf[StructType]
  }

  /**
    * 获取二级schema
    * @param subject
    * @return
    */
  def getDataLakeSchema(subject:String):StructType = {
    val baseSchemaRepository = SchemaRepositoryFactory.initSchemaRepository("test001","http://10.200.8.35:8081", SchemaRepositoryType.REMOTE)
    val schemaMetadata = baseSchemaRepository.getLatestSchemaMetadata(subject)
    SchemaConverters.toSqlType(schemaMetadata.getSchema).dataType.asInstanceOf[StructType]
  }

}
