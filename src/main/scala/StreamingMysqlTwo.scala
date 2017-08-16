
import java.util.Properties

import com.alibaba.fastjson.JSON
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


object StreamingMysqlTwo {

  //  case class PlaneInfo(offset: String)
  //  连接Mysql配置参数
  //  lazy val url = "jdbc:mysql://10.199.107.240:5508/om"
  //  lazy val username = "crm"
  //  lazy val password = "crm"

  //累计算法
  val updateFunc = (iter: Iterator[(String, Seq[Double], Option[Double])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0.0)).map(i => (x, i)) }
  }

//  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
//    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
//    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
//  }

  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("StreamingMysqlTwo").setMaster("local[2]")
    //设置允许多个sparkconf对象运行
    //    sparkConf.set("spark.driver.allowMultipleContexts", "true")

    //----------------- 设置从Mysql中取最后一次插入的数据----------------
    //获取context
    /* val sc = new SparkContext(sparkConf)
     //获取sqlContext
     val sqlContext = new SQLContext(sc)
     //创建jdbc连接信息
     val uri = url + "?user=" + username + "&password=" + password + "&useUnicode=true&characterEncoding=UTF-8"
     val prop = new Properties()
     //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误
     prop.put("driver", "com.mysql.jdbc.Driver")
     //加载mysql数据表
     //    val df_test1: DataFrame = sqlContext.read.jdbc(uri, "user_t", prop)
     val df_test2: DataFrame = sqlContext.read.jdbc(uri, "dm_sstreaming_getdata_test", prop)

     //获取最后一行数据
     val lastData = df_test2.select("insert_time", "click_sum").collect().last
     val beforSum = Some(lastData.get(1)).getOrElse(0)*/

    //---------------streaming代码------------------
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("/Users/liushuai/Desktop/temp/kafkalog2")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val dstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = dstream.flatMap(line => {
      val data = JSON.parseObject(line._2)
      Some(data)
    })

    // ---------- 过滤数据取只是inset的json数据 ----------
    //只需要的表
    val onlyTable = lines.filter(x => x.getJSONObject("header").getString("tableName").equals("dm_liushuai_sstreaming_test"))
    //只需要表中的特定操作

    val onlyLine = onlyTable.filter {
      //        x => x.getJSONObject("header").getString("eventType").equals("INSERT") || x.getJSONObject("header").getString("eventType").equals("UPDATE")
      x => x.getJSONObject("header").getString("eventType").equals("INSERT")
    }
    //   过滤数据 取只是Y的json数据
    val rowList = onlyLine.map(x => x.getJSONArray("rowList"))
    val yRowList = rowList.filter(x => x.getJSONObject(0).getJSONArray("afterColumns").getJSONObject(35).getString("value").equals("Y"))
    //   进行取数计算
    val obj = yRowList.map(x => x.getJSONObject(0))
    val after = obj.map(x => x.getJSONArray("afterColumns"))
    val after27 = after.map(x => x.getJSONObject(27))
    //  val valAfterafter27 = after27.map(x => ("amt_sum", x.getString("value").toInt))
        val valAfterafter27 = after27.map(x => ("amt_sum", x.getString("value").toDouble))
    //   计算点击总数
    val amt_sum = valAfterafter27.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    //--------------插入mysql数据库---------------
    /* click_Counts.foreachRDD(rdd => {
       rdd.foreachPartition(partitionOfRecords => {
         val conn = ConnectPool.getConnection
         conn.setAutoCommit(false); //设为手动提交
         try {
           val stmt = conn.createStatement();

           partitionOfRecords.foreach(record => {
             stmt.addBatch("insert into dm_sstreaming_getdata_test (insert_time,click_sum) values (now()," + record._2 + ")");

 //            stmt.addBatch("insert into dm_sstreaming_getdata_test (insert_time,click_sum) values (now()," + record._2 + " + " + beforSum + ")");
           })

           stmt.executeBatch();
           conn.commit(); //提交事务
         }
         finally {
           conn.close();
         }
       })
     })*/
    amt_sum.print()
    //  amt_sum.print()

    ssc.start()
    ssc.awaitTermination()
  }

}

