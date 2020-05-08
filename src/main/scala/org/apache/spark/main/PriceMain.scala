package org.apache.spark.main

import org.apache.spark.constants.PriceConstants
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object PriceMain {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.dir.home", PriceConstants.winutils_File_Path)
    val conf = new SparkConf()
      .setAppName("Spark")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
    val data = sqlContext.read.option("header", "true").option("inferSchema", "true").csv(PriceConstants.file_Path)
    data.show(10)
    data.printSchema()
    val stringFeatureColumns = Array("MSZoning", "LotFrontage", "Street", "Alley", "LotShape", "LandContour", "Utilities", "LotConfig", "LandSlope", "Neighborhood", "Condition1", "Condition2", "BldgType", "HouseStyle", "RoofStyle", "RoofMatl", "Exterior1st", "Exterior2nd", "MasVnrType", "MasVnrArea", "ExterQual", "ExterCond", "Foundation", "BsmtQual", "BsmtCond", "BsmtExposure", "BsmtFinType1", "BsmtFinType2", "Heating", "HeatingQC", "CentralAir", "Electrical", "KitchenQual", "Functional", "FireplaceQu", "GarageType", "GarageYrBlt", "GarageFinish", "GarageQual", "GarageCond", "PavedDrive", "PoolQC", "Fence", "MiscFeature", "SaleType", "SaleCondition")

    val indexer = stringFeatureColumns.map(colName => new StringIndexer().setInputCol(colName).setOutputCol(colName + "_indexed"))

    val pipeline = new Pipeline()
      .setStages(indexer)
    val stringIndexed = pipeline.fit(data).transform(data)
    stringIndexed.show()
    val numericColumns = Array("Id", "MSSubClass", "LotArea", "OverallQual", "OverallCond", "YearBuilt", "YearRemodAdd", "BsmtFinSF1", "BsmtFinSF2", "BsmtUnfSF", "TotalBsmtSF", "1stFlrSF", "2ndFlrSF", "LowQualFinSF", "GrLivArea", "BsmtFullBath", "BsmtHalfBath", "FullBath", "HalfBath", "BedroomAbvGr", "KitchenAbvGr", "TotRmsAbvGrd", "Fireplaces", "GarageCars", "GarageArea", "WoodDeckSF", "OpenPorchSF", "EnclosedPorch", "3SsnPorch", "ScreenPorch", "PoolArea", "MiscVal", "MoSold", "YrSold", "MSZoning_indexed", "LotFrontage_indexed", "Street_indexed", "Alley_indexed", "LotShape_indexed", "LandContour_indexed", "Utilities_indexed", "LotConfig_indexed", "LandSlope_indexed", "Neighborhood_indexed", "Condition1_indexed", "Condition2_indexed", "BldgType_indexed", "HouseStyle_indexed", "RoofStyle_indexed", "RoofMatl_indexed", "Exterior1st_indexed", "Exterior2nd_indexed", "MasVnrType_indexed", "MasVnrArea_indexed", "ExterQual_indexed", "ExterCond_indexed", "Foundation_indexed", "BsmtQual_indexed", "BsmtCond_indexed", "BsmtExposure_indexed", "BsmtFinType1_indexed", "BsmtFinType2_indexed", "Heating_indexed", "HeatingQC_indexed", "CentralAir_indexed", "Electrical_indexed", "KitchenQual_indexed", "Functional_indexed", "FireplaceQu_indexed", "GarageType_indexed", "GarageYrBlt_indexed", "GarageFinish_indexed", "GarageQual_indexed", "GarageCond_indexed", "PavedDrive_indexed", "PoolQC_indexed", "Fence_indexed", "MiscFeature_indexed")

    val assembler = new VectorAssembler().setInputCols(numericColumns).setOutputCol("features")

    val transformedData = assembler.transform(stringIndexed)
    val featureData = transformedData.select($"features", $"SalePrice".alias("Actual_SalePrice"))
    featureData.show()
    val split = featureData.randomSplit(Array(0.7, 0.3))
    val train = split(0)
    val test = split(1)
    val trainCount = train.count
    val testCount = test.count
    val lr = new LinearRegression().setLabelCol("Actual_SalePrice").setFeaturesCol("features").setMaxIter(10).setRegParam(0.3)
    val model = lr.fit(train)
    println("Model Trained Successfully")
    val prediction = model.transform(test)
    val results = prediction.select($"features", $"Actual_SalePrice", $"prediction".alias("Predicted_SalePrice"))
    results.show()
    results.createOrReplaceTempView("model")

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("Actual_SalePrice")
      .setPredictionCol("Predicted_SalePrice")
    val RMSE = evaluator.evaluate(results)
    println("Root Mean Square Error Value :" + RMSE)
  }
}