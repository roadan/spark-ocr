package com.rodenski.spark.ocr

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, SchemaRelationProvider, RelationProvider}

/**
  * Created by roadan on 5/17/16.
  */
class DefaultSource extends RelationProvider
  //with SchemaRelationProvider
  //with CreatableRelationProvider
  {

  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {

    val path = checkPath(parameters)
    ImageTextRelation(path)(sqlContext)

  }

  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path", sys.error("'path' must be specified for CSV data."))
  }

  //override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = ???

  //override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = ???
}
