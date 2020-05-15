package org.apache.spark.main

import org.apache.log4j.{Level, Logger}
import org.apache.spark.constants.PriceConstants
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object PriceMain {

  val conf = new SparkConf()
    .setAppName("Spark")
    .setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.dir.home", PriceConstants.winutils_File_Path)

    val data = sqlContext.read.option("header", "true").option("inferSchema", "true").csv(PriceConstants.file_Path)
    data.show(10)
    data.printSchema()

    val stringIndexer: NumericConversion = new NumericConversion()
    val stringIndexed = stringIndexer.numeric(data)

    val feature: Feature = new Feature()
    val featureData = feature.features(stringIndexed)

    val split = featureData.randomSplit(Array(0.7, 0.3))
    val train = split(0)
    val test = split(1)

    val error: LinearModel = new LinearModel()
    val RMSE: String = error.predictorModel(train, test)
    println("Root Mean Square Error Value :" + RMSE)
  }

}
