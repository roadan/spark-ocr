package com.rodenski.spark.ocr

import java.awt.image.BufferedImage
import java.io.{ByteArrayInputStream, IOException}
import javax.imageio.ImageIO

import net.sourceforge.tess4j.Tesseract
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{TableScan, BaseRelation}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

/**
  * Created by roadan on 5/17/16.
  */
case class ImageTextRelation(path: String)(@transient val sqlContext: SQLContext) extends BaseRelation with TableScan {

  override def schema = {
    StructType(Seq(
      StructField("document_name", DataTypes.StringType, nullable = true),
      StructField("text", DataTypes.StringType, nullable = true)
    ))
  }

  override def buildScan: RDD[Row] = {

    val tess = new Tesseract

    // creating an RDD of (String, String)
    val files = sqlContext.sparkContext.binaryFiles(path)
    val data = files.map((f)=>{
      (f._1, tess.doOCR(toBufferedImage(f._2.toArray())))
    })

    // transforming the RDD[(String, String)] to RDD[Row]
    val result = data.map(r=>Row.fromTuple(r))
    result
  }

  private def toBufferedImage(image: Array[Byte]): BufferedImage = {
    ImageIO.read(new ByteArrayInputStream(image))
  }

}
