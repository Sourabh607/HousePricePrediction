package org.apache.spark.main

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.DataFrame

class NumericConversion {

  /*
 * Method to convert all the string type feature columns into indexed form
  as VectorAssembler takes numeric, binary or vector as input
  * Defining pipeline
   */
  def numeric(data: DataFrame): DataFrame = {

    val stringFeatureColumns = Array("MSZoning", "LotFrontage", "Street", "Alley", "LotShape", "LandContour", "Utilities", "LotConfig", "LandSlope", "Neighborhood", "Condition1", "Condition2", "BldgType", "HouseStyle", "RoofStyle", "RoofMatl", "Exterior1st", "Exterior2nd", "MasVnrType", "MasVnrArea", "ExterQual", "ExterCond", "Foundation", "BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1", "BsmtFinType2", "Heating", "HeatingQC", "CentralAir", "Electrical", "KitchenQual", "Functional", "FireplaceQu", "GarageType", "GarageYrBlt", "GarageFinish", "GarageQual", "GarageCond", "PavedDrive", "PoolQC", "Fence", "MiscFeature", "SaleType", "SaleCondition")
    val indexer = stringFeatureColumns.map(colName => new StringIndexer().setInputCol(colName).setOutputCol(colName + "_indexed"))

    val pipeline = new Pipeline()
      .setStages(indexer)
    val stringIndexed = pipeline.fit(data).transform(data)
    /* stringIndexed.coalesce(1).write.format("csv")
       .option("header","true").option("inferSchema","true").option("delimeter",",").save("E:/DataSamples/csv/indexedTest.csv")
    */
    stringIndexed.show()
    return stringIndexed
  }
}
