import java.text.SimpleDateFormat


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


case  class RecordLog(accessTime:String , website:String , sourceIp:String  ,  targetUrl:String  ,   statusAcc:String )

case  class RecordLog2( accessDate:String, accessTime:String , website:String , sourceIp:String  ,  targetUrl:String  ,   statusAcc:String   )


/**
  * Created by Dawson on 2017/1/31.
  */
object WebLogProcess  extends  App {

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

  mapData.take(30).foreach(arr => {
    arr.foreach(a => print(a + " "))
    println()
  })

  // val   recordRdd = mapData.map( arr => RecordLog( arr(0) , arr(1) ,  arr(2)  ,  arr(12)  ,  arr(8)))

  val recordRdd2 = mapData.map(arr => {
    val dateArr = arr(0).split("T")
    val timeStr :String = dateArr(1).substring(0, dateArr(1).indexOf(".")).replace(":", "")
    val sourceIp = arr(2).split(":")(0)
    RecordLog2(dateArr(0), dateArr(0).replace("-", "") + timeStr.toString, arr(1), sourceIp, arr(12), arr(8))
  })

  //val recordLog3 = recordRdd2.filter(rd => rd.accessTime.length == 14 && rd.accessTime.matches("\\d+"))


  val DF = spark.createDataFrame(recordRdd2)
  /// DF.orderBy("sourceIp", "accessTime").show(100)
  // DF.registerTempTable("recordlog")
  //spark.sqlContext.sql(("select   max( accessDate ) , min( accessDate ) from  recordlog ")).show()
  // println(  DF.count() )


  // Sessionize the web log by IP. Sessionize = aggregrate all page hits by visitor/IP during a fixed time window.
  DF.createOrReplaceTempView("recordlog")
  spark.sqlContext.udf.register("strlen" ,  (str:String )=> str.length )

  //spark.sql("select  *from  recordlog  where  strlen(accessTime)!=14  ")

  val SessionizeDF = spark.sqlContext.sql("select  min( accessTime )  starttime,  max( accessTime )  endtime ,  sourceIp ,  " +
    "count(targetUrl) cnt from recordlog  group by  sourceIp")



// SessionizeDF.show( 100 )
  spark.sqlContext.udf.register("calc_time",  ( st:String , ed:String ) => {
    //println( "str::" + st + ":::" + ed )
    val sdf = new SimpleDateFormat("YYYYMMDDHHmmss")
    var     t1=sdf.parse( st.trim  ).getTime
    var     t2=sdf.parse( ed.trim ).getTime

    (t2-t1)/1000

  })

  SessionizeDF.createOrReplaceTempView("sessionview")

  /***
  *   Determine the average session time
  */

  val  PV1 = spark.sqlContext.sql("select   starttime,  endtime ,  calc_time(  starttime ,  endtime )   session_time " +
    ",   calc_time(  starttime ,  endtime ) / cnt  avg , cnt  ,  sourceIp " +
    "    from   sessionview  ")


  PV1.write.csv("F:\\app\\data\\pv1")
  /***
  *  Determine unique URL visits per session. To clarify, count a hit to a unique URL only once per session.
  */

  val  uniqueSesssionDF =  spark.sql("select  *  from  sessionview    where  cnt=1")


  uniqueSesssionDF.show(100)

  val  countSession= uniqueSesssionDF.count()

  println( "count ::" + countSession )


  /***
  *   Find the most engaged users, ie the IPs with the longest session times
  */

  PV1.createOrReplaceTempView("pageview1")



  val  mostEngageDF=  spark.sql("select  * from  pageview1  order  by  session_time  desc")



  mostEngageDF.show(100)
  mostEngageDF.limit(100).write.csv("F:\\app\\data\\result2")


}
