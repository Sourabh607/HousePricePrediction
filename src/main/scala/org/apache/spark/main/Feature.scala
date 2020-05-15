package org.apache.spark.main

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SQLContext}

class Feature {

  /*
  Combining all feature colummns into single single
   */
  def features(stringIndexed: DataFrame): DataFrame = {
    import PriceMain.sqlContext.implicits._
    val numericColumns = Array("Id", "MSSubClass", "LotArea", "OverallQual", "OverallCond", "YearBuilt", "YearRemodAdd", "BsmtFinSF1", "BsmtFinSF2", "BsmtUnfSF", "TotalBsmtSF", "1stFlrSF", "2ndFlrSF", "LowQualFinSF", "GrLivArea", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath", "BedroomAbvGr", "KitchenAbvGr", "TotRmsAbvGrd", "Fireplaces", "GarageCars", "GarageArea", "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", "ScreenPorch", "PoolArea", "MiscVal", "MoSold", "YrSold", "MSZoning_indexed", "LotFrontage_indexed", "Street_indexed", "Alley_indexed", "LotShape_indexed", "LandContour_indexed", "Utilities_indexed", "LotConfig_indexed", "LandSlope_indexed", "Neighborhood_indexed", "Condition1_indexed", "Condition2_indexed", "BldgType_indexed", "HouseStyle_indexed", "RoofStyle_indexed", "RoofMatl_indexed", "Exterior1st_indexed", "Exterior2nd_indexed", "MasVnrType_indexed", "MasVnrArea_indexed", "ExterQual_indexed", "ExterCond_indexed", "Foundation_indexed", "BsmtQual_indexed", "BsmtCond_indexed", "BsmtExposure_indexed", "BsmtFinType1_indexed", "BsmtFinType2_indexed", "Heating_indexed", "HeatingQC_indexed", "CentralAir_indexed", "Electrical_indexed", "KitchenQual_indexed", "Functional_indexed", "FireplaceQu_indexed", "GarageType_indexed", "GarageYrBlt_indexed", "GarageFinish_indexed", "GarageQual_indexed", "GarageCond_indexed", "PavedDrive_indexed", "PoolQC_indexed", "Fence_indexed", "MiscFeature_indexed")
    val assembler = new VectorAssembler().setInputCols(numericColumns).setOutputCol("features")
    val transformedData = assembler.transform(stringIndexed)
    val featureData = transformedData.select($"features", $"SalePrice".alias("Actual_SalePrice"))
    featureData.show(2,false)
    return featureData
  }
}
