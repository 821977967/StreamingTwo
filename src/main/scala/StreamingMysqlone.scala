
import com.alibaba.fastjson.JSON
import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils


object StreamingMysqlone {
  //  case class PlaneInfo(offset: String)

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    iter.flatMap { case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i)) }
  }

  def main(args: Array[String]) {
    LoggerLevels.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("LSKafkaWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    ssc.checkpoint("/Users/liushuai/Desktop/temp/kafkalogs")

    //"alog-2016-04-16,alog-2016-04-17,alog-2016-04-18"
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val dstream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)
    val lines = dstream.flatMap(line => {
      val data = JSON.parseObject(line._2)
      Some(data)
    })
    //    过滤数据取只是inset的json数据
    val lineIn = lines.filter(x => x.getJSONObject("header").getString("eventType").equals("INSERT"))
    //    过滤数据 取只是Y的json数据
    val rowList = lineIn.map(x => x.getJSONArray("rowList"))
    val yRowList = rowList.filter(x => x.getJSONObject(0).getJSONArray("afterColumns").getJSONObject(2).getString("value").equals("Y"))
    val obj = yRowList.map(x => x.getJSONObject(0))
    val after = obj.map(x => x.getJSONArray("afterColumns"))
    val after1 = after.map(x => x.getJSONObject(1))
    val valAfter1 = after1.map(x => ("click_Counts", x.getString("value").toInt))
    //    计算点击总数
    val click_Counts = valAfter1.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    //插入mysql数据库
    click_Counts.foreachRDD(rdd => {
      rdd.foreachPartition(partitionOfRecords => {
        val conn = ConnectPool.getConnection
        conn.setAutoCommit(false); //设为手动提交
        try{
        val stmt = conn.createStatement();

        partitionOfRecords.foreach(record => {

          stmt.addBatch("insert into dm_sstreaming_getdata_test (insert_time,click_sum) values (now(),'" + record._2 + "')");
        })

        stmt.executeBatch();
        conn.commit(); //提交事务
        }
        finally {
          conn.close();
        }
      })
    })

    click_Counts.print()

    ssc.start()
    ssc.awaitTermination()

  }

}
