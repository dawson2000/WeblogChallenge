import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.regression.{GBTRegressor, RandomForestRegressor}
import org.apache.spark.sql.SparkSession

/**
  * Created by Dawson on 2017/2/3.
  */
object WebLogUniqueUrlModel  extends   App{

  val config = new SparkConf().setAppName("web-data-app").setMaster("local[8]")


  config.set("spark.driver.maxResultSize", "10g").set("spark.executor.memory", "2g")
  config.set("spark.local.dir", "F:\\CurrencyTrade\\temp\\local\\")
  config.set("spark.sql.warehouse.dir", "F:\\CurrencyTrade\\temp\\share\\")
  config.set("spark.sql.crossJoin.enabled", "true")
  val spark = SparkSession.builder().config(config).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")
  val webDataRdd = spark.sparkContext.textFile("E:\\developer\\WeblogChallenge\\data\\2015_07_22_mktplace_shop_web_log_sample.log")

  // data  stitch
  val mapData = webDataRdd.map(row => row.replaceAll("\\\"", "").split(" ").take(13))

  val recordRdd2 = mapData.map(arr => {
    val dateArr = arr(0).split("T")
    val timeStr :String = dateArr(1).substring(0, dateArr(1).indexOf(".")).replace(":", "")
    val sourceIp = arr(2).split(":")(0)
    RecordLog2(dateArr(0), dateArr(0).replace("-", "") + timeStr.toString, arr(1), sourceIp, arr(12), arr(8))
  })

  val DF = spark.createDataFrame(recordRdd2)
  //DF.show()
  /***
    *
    */

  val  uniqueCntDF = DF.distinct().groupBy("sourceIp").count()
  uniqueCntDF.show()

  val  targetRdd = uniqueCntDF.rdd.map(row=>{
    IpSession( Vectors.dense( row.getString(0).split("\\.").map(e=>e.toDouble ) ) ,  row.getLong(1).toInt )
  })

  val  targetDF = spark.createDataFrame( targetRdd  )

  targetDF.show()

  val rf = new RandomForestRegressor()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")
  // .setMaxDepth(15).setMaxBins(5)

  val Array(trainingData, testData) = targetDF.randomSplit(Array(0.7, 0.3))

  val model=rf.fit(trainingData)
  val   predictionDF = model.transform(testData )

  predictionDF.show(100)
  //org.apache.spark.ml.linalg.VectorUDT
  //org.apache.spark.mllib.linalg.VectorUDT

  val evaluator = new RegressionEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("rmse")
  val rmse = evaluator.evaluate(predictionDF)
  println("RandomForestRegressor  ::  Root Mean Squared Error (RMSE) on test data = " + rmse)


  /***
    *  using   Gradient-boosted tree regression  create  model
    *
    *
    */
  val gbt = new GBTRegressor()
    .setLabelCol("label")
    .setFeaturesCol("indexedFeatures")
    .setMaxIter(10).setMaxDepth(10)

  val gbtModel = gbt.fit( trainingData )
  val gbtPredictDF = gbtModel.transform( testData )

  gbtPredictDF.show()

  val rmse2 = evaluator.evaluate(gbtPredictDF)

  println("GBTRegressor  ::  Root Mean Squared Error (RMSE) on test data = " + rmse2)


  spark.stop()






}
