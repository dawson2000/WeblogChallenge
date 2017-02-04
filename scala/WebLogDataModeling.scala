import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util

import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Dawson on 2017/2/2.
  */
object WebLogDataModeling  extends  App {

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

/**
  workLoadDF.show()

   val   lines=new  util.ArrayList[String]()
  lines.add("time,workload")
     workLoadDF.rdd.filter(row=>row.getString(0).isEmpty==false)foreach(row=>{
       lines.add( row.mkString(",") )
   })*/

// IOUtils.writeLines(lines ,"" ,new FileOutputStream("F:\\app\\data\\workload\\works.csv"))
// val  outStream = new FileOutputStream("F:\\app\\data\\workload\\works.csv")
//   IOUtils.writeLines( lines ,  null  , outStream )

  /***
    *    Predict the session length for a given IP
    */
  DF.createOrReplaceTempView("recordlog")

  val SessionizeDF = spark.sqlContext.sql("select  min( accessTime )  starttime,  max( accessTime )  endtime ,  sourceIp  from recordlog  group by  sourceIp")

  spark.sqlContext.udf.register("calc_time",  ( st:String , ed:String ) => {
    //println( "str::" + st + ":::" + ed )
    val sdf = new SimpleDateFormat("YYYYMMDDHHmmss")
    var     t1=sdf.parse( st.trim  ).getTime
    var     t2=sdf.parse( ed.trim ).getTime

    (t2-t1)/1000
  })

  SessionizeDF.createOrReplaceTempView("sessionlog")

  val  SessionLogDF =  spark.sql("select  calc_time(  starttime , endtime )  sessionlength , sourceIp  from    sessionlog  " +
    "where  starttime is not null  and   endtime is not null   and   starttime!=''")

  SessionLogDF.show()
  println( "SessionLogDF ::: " +  SessionLogDF.count() )

  val   lines=new  util.ArrayList[String]()
  lines.add("sessionlen,ip1,ip2,ip3,ip4")
  SessionLogDF.rdd.foreach(row=>{
    lines.add( row.mkString(",").replaceAll("\\.",",") )
  })

   val  outStream = new FileOutputStream("F:\\app\\data\\workload\\sessionlength2.csv")
   IOUtils.writeLines( lines ,  null  , outStream )


  /***
    *   Predict the number of unique URL visits by a given IP
    */




}
