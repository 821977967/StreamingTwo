import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** * 使用Scala开发集群运行的Spark 在线黑名单过滤程序 * *
  * 背景描述：在广告点击计费系统中，我们在线过滤掉黑名单的点击， 进而保护广告商的利益，只进行有效的广告点击计费 * 或者在防刷评分（或者流量）系统，过滤掉无效的投票或者评分或者流量； *
  * 实现技术：使用transform Api直接基于RDD编程，进行join操作 * */



object OnlineBlackListFilter {
  def main(args: Array[String]){
  val conf = new SparkConf() //创建SparkConf对象
  conf.setAppName("OnlineBlackListFilter")
  // 设置应用程序的名称，在程序运行的监控界面可以看到名称
  conf.setMaster("spark://Master:7077")
  // 此时，程序在Spark集群
  val ssc = new StreamingContext(conf, Seconds(30))
  /** * 黑名单数据准备，实际上黑名单一般都是动态的，例如在Redis或者数据库中，黑名单的生成往往有复杂的业务 * 逻辑，具体情况算法不同，
    * 但是在Spark Streaming进行处理的时候每次都能工访问完整的信息 */
  val blackList = Array(("hadoop", true), ("mahout", true))
  val blackListRDD = ssc.sparkContext.parallelize(blackList, 8)
  val adsClickStream = ssc.socketTextStream("Master", 9999) /** * 此处模拟的广告点击的每条数据的格式为：time、name * 此处map操作的结果是name、（time，name）的格式 */
  val adsClickStreamFormatted = adsClickStream.map { ads => (ads.split(" ")(1), ads) }
  adsClickStreamFormatted.transform(userClickRDD => {
    //通过leftOuterJoin操作既保留了左侧用户广告点击内容的RDD的所有内容，又获得了相应点击内容是否在黑名单中
    val joinedBlackListRDD = userClickRDD.leftOuterJoin(blackListRDD)
    /** * 进行filter过滤的时候，其输入元素是一个Tuple：（name,((time,name), boolean)） * 其中第一个元素是黑名单的名称，第二元素的第二个元素是进行leftOuterJoin的时候是否存在在值 * 如果存在的话，表面当前广告点击是黑名单，需要过滤掉，否则的话则是有效点击内容； */
    val validClicked = joinedBlackListRDD.filter(joinedItem => {
      if(joinedItem._2._2.getOrElse(false)) { false } else { true } })
    validClicked.map(validClick => {validClick._2._1}) }).print
  /** * 计算后的有效数据一般都会写入Kafka中，下游的计费系统会从kafka中pull到有效数据进行计费 */
  ssc.start()
  ssc.awaitTermination()
}

   /* 作者
    ： 海纳百川_spark
    链接
    ： http :// www.jianshu.com / p / 8d e6ec8513ca
      來源
    ： 简书
    著作权归作者所有
    。 商业转载请联系作者获得授权
    ， 非商业转载请注明出处
    。*/

  }
