package org.apache.spark.main

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

class NumericConversionTest extends CommonTest {

/* val conf = new SparkConf()
    .setAppName("Test1")
    .setMaster("local")
   .set("spark.driver.allowMultipleContexts", "true")
  val sc1 = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)*/
  /*
  Test for verifying numeric method in NummericConversion Class
   */
  "numeric" should "return dataset by indexing string columns to numeric values" in {

    val df = PriceMain.sqlContext.read.option("header","true").option("inferSchema", "true").csv("salesDataTest.csv")
    val num : NumericConversion = new NumericConversion()
    val transformedDf = num.numeric(df)
     val output = transformedDf.filter(transformedDf("Id").equalTo("1")).select("MSZoning_indexed").collectAsList()
    val author = output.get(0).toString()
     assertResult(expected = "[0.0]")(author)
  }
}
