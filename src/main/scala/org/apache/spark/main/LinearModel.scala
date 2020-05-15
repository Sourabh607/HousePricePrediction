package org.apache.spark.main

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.DataFrame

class LinearModel {

  /*
 * Declaring Linear regression ML model
 * Training ML model using Training data
 * Testing ML model using Test data
   */
  def predictorModel(train: DataFrame, test: DataFrame): String = {
    import PriceMain.sqlContext.implicits._
    val lr = new LinearRegression().setLabelCol("Actual_SalePrice").setFeaturesCol("features").setMaxIter(10).setRegParam(0.3)
    val model = lr.fit(train)
    println("Model Trained Successfully")
    val prediction = model.transform(test)
    val results = prediction.select($"features", $"Actual_SalePrice", $"prediction".alias("Predicted_SalePrice"))
    results.show()

    val evaluator = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("Actual_SalePrice")
      .setPredictionCol("Predicted_SalePrice")
    val RMSE = evaluator.evaluate(results)
    return RMSE.toString
  }
}
