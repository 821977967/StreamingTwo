
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingGetMysqlTest {
  //数据库配置
//  lazy val url = "jdbc:mysql://your_ip:3306/my_test"
//  lazy val username = "root"
//  lazy val password = "secret_password"

  lazy val url = "jdbc:mysql://10.199.107.240:5508/om"
  lazy val username = "crm"
  lazy val password = "crm"

  def main(args: Array[String]) {
    //    val sparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local[2]").set("spark.app.id", "sql")
    val sparkConf = new SparkConf().setAppName("sparkSqlTest").setMaster("local[2]")
    //序列化,若不设置,则为默认值。不影响程序正常执行
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    sparkConf.set("spark.kryoserializer.buffer", "256m")
//    sparkConf.set("spark.kryoserializer.buffer.max", "2046m")
//    sparkConf.set("spark.akka.frameSize", "500")
//    sparkConf.set("spark.rpc.askTimeout", "30")
    //获取context
    val sc = new SparkContext(sparkConf)
    //获取sqlContext
    val sqlContext = new SQLContext(sc)

    //引入隐式转换，可以使用spark sql内置函数
    import sqlContext.implicits._

    //创建jdbc连接信息
    val uri = url + "?user=" + username + "&password=" + password + "&useUnicode=true&characterEncoding=UTF-8"
    val prop = new Properties()
    //注意：集群上运行时，一定要添加这句话，否则会报找不到mysql驱动的错误
    prop.put("driver", "com.mysql.jdbc.Driver")
    //加载mysql数据表
//    val df_test1: DataFrame = sqlContext.read.jdbc(uri, "user_t", prop)
    val df_test2: DataFrame = sqlContext.read.jdbc(uri, "dm_liushuai_sstreaming_test", prop)

    //从dataframe中获取所需字段
    df_test2.select("id", "click_count", "status").collect()
      .foreach(row => {
        println("id  " + row(0) + " ,click_count  " + row(1) + ", status  " + row(2))
      })
    //注册成临时表
//    df_test1.registerTempTable("temp_table")
//
//    val total_sql = "select * from temp_table "
//    val total_df: DataFrame = sqlContext.sql(total_sql)
//
//    //将结果写入数据库中
//    val properties=new Properties()
//    properties.setProperty("user","root")
//    properties.setProperty("password","secret_password")
//    total_df.write.mode("append").jdbc("jdbc:mysql://your_ip:3306/my_test?useUnicode=true&characterEncoding=UTF-8","t_result",properties)

    /**
      * 注意：查看源码可以知道详细意思
    def mode(saveMode: String): DataFrameWriter = {
          this.mode = saveMode.toLowerCase match {
          case "overwrite" => SaveMode.Overwrite
          case "append" => SaveMode.Append
          case "ignore" => SaveMode.Ignore
          case "error" | "default" => SaveMode.ErrorIfExists
          case _ => throw new IllegalArgumentException(s"Unknown save mode: $saveMode. " +
            "Accepted modes are 'overwrite', 'append', 'ignore', 'error'.")
    }
      */

    //分组后求平均值
//    total_df.groupBy("name").avg("age").collect().foreach(x => {
//      println("name " + x(0))
//      println("age " + x(1))
//    })

  }
}


