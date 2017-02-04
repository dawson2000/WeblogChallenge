import java.text.SimpleDateFormat

import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.regression.{GBTRegressor, RandomForestRegressionModel, RandomForestRegressor}
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.linalg.{Vector, Vectors}

case  class  IpSession( indexedFeatures:Vector, label: Int )

/**
  * Created by Dawson on 2017/2/3.
  */
object WebLogSessionLengthModel  extends   App{

  /***
    *   Predict the expected load (requests/second) in the next minute
    */

  val config = new SparkConf().setAppName("web-data-app").setMaster("local[8]")


  config.set("spark.driver.maxResultSize", "10g").set("spark.executor.memory", "2g")
  config.set("spark.local.dir", "F:\\CurrencyTrade\\temp\\local\\")
  config.set("spark.sql.warehouse.dir", "F:\\CurrencyTrade\\temp\\share\\")
  config.set("spark.sql.crossJoin.enabled", "true")


  val spark = SparkSession.builder().config(config).getOrCreate()
  val webDataRdd = spark.sparkContext.textFile("E:\\developer\\WeblogChallenge\\data\\2015_07_22_mktplace_shop_web_log_sample.log")

  /// data  observer
  webDataRdd.take(20).foreach(println(_))

  // data  stitch
  val mapData = webDataRdd.map(row => row.replaceAll("\\\"", "").split(" ").take(13))

  val recordRdd2 = mapData.map(arr => {
    val dateArr = arr(0).split("T")
    val timeStr :String = dateArr(1).substring(0, dateArr(1).indexOf(".")).replace(":", "")
    val sourceIp = arr(2).split(":")(0)
    RecordLog2(dateArr(0), dateArr(0).replace("-", "") + timeStr.toString, arr(1), sourceIp, arr(12), arr(8))
  })

  val DF = spark.createDataFrame(recordRdd2)
  DF.show()

  val  workLoadDF = DF.groupBy("accessTime").count().orderBy("accessTime")

  DF.createOrReplaceTempView("recordlog")

  val SessionizeDF = spark.sqlContext.sql("select  min( accessTime )  starttime,  max( accessTime )  endtime ,  sourceIp  from recordlog  group by  sourceIp")

  spark.sqlContext.udf.register("calc_time",  ( st:String , ed:String ) => {
    //println( "str::" + st + ":::" + ed )
    val sdf = new SimpleDateFormat("YYYYMMDDHHmmss")
    var     t1=sdf.parse( st.trim  ).getTime
    var     t2=sdf.parse( ed.trim ).getTime

      ( (t2-t1)/1000).toString
  })

  SessionizeDF.createOrReplaceTempView("sessionlog")

  val  SessionLogDF =  spark.sql("select  calc_time(  starttime , endtime )  sessionlength , sourceIp  from    sessionlog  " +
    "where  starttime is not null  and   endtime is not null   and   starttime!=''")

  SessionLogDF.show()
  println( "SessionLogDF ::: " +  SessionLogDF.count() )

  val  rdd = SessionLogDF.rdd.map(row=>{

      IpSession(   Vectors.dense( row.getString(1).split("\\.").map(e=>e.toDouble ) ) ,  row.getString(0).toInt )
  })

  //println(rdd.take(10).foreach(t=>println(t._1.mkString("#")  + "@" + t._2)))

  val  targetDF = spark.createDataFrame(rdd)
  targetDF.show()

 /* val featureIndexer = new VectorIndexer()
    .setInputCol("features")
    .setOutputCol("indexedFeatures")
    .setMaxCategories(4)
    .fit(targetDF)

  val  VDF = featureIndexer.transform(targetDF)*/

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
