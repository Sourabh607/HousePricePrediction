package org.apache.spark.main

class NumericConversionTest extends CommonTest {

  /*
  * Test for verifying numeric method in NummericConversion Class
   */
  "numeric" should "return dataset by indexing string columns to numeric values" in {

    val df = PriceMain.sqlContext.read.option("header", "true").option("inferSchema", "true").csv("salesDataTest.csv")
    val num: NumericConversion = new NumericConversion()
    val transformedDf = num.numeric(df)
    val output = transformedDf.filter(transformedDf("Id").equalTo("1")).select("MSZoning_indexed").collectAsList()
    val author = output.get(0).toString()
    assertResult(expected = "[0.0]")(author)
  }
}
