package org.apache.spark.main

class FeatureTest extends CommonTest {

  "Feature Combiner" should "combine all features in single feature column" in {
    val indexed =  PriceMain.sqlContext.read.option("header", "true").option("inferSchema", "true").csv("/indexedTest.csv")
    val feat : Feature = new Feature()
    val df = feat.features(indexed)
    val output = df.select("Actual_SalePrice").collectAsList()
    val result = output.get(0)(0)
    //println(result)
    assert(208500 == result)
  }
}
