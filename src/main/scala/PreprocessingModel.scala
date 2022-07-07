package SparkOnHDFS

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


/**
 *
 *
 *  数据处理阶段
 */

object PreprocessingModel {


  def main(args: Array[String]): Unit = {


    //1.创建Spark环境配置对象
    val conf = new SparkConf().setAppName("SparkMovie").setMaster("local")
    //2.创建SparkSession对象
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    spark.read.format("csv")
      .option("header", true)
      .option("multiLine", true)
      .load("file:///D:\\data\\基站经纬度数据.csv")

    val data_path_c = "file:///D:\\data\\基站经纬度数据.csv"
    //读取静态数据
    spark.read.format("csv")
      .option("delimiter", ",") //分隔符
      .option("qoute", "")
      .option("nullValue", "000")
      .load(data_path_c)
      .select("_c0","_c1","_c2")
      .toDF("longitude","latitude","laci")
      .createTempView("static_table")


    val data_path_k="file:///D:\\data\\原始数据.csv"
    //读取通信数据
    spark.read.format("csv")
      .option("delimiter", ",") //分隔符
      .option("qoute", "")
      .option("nullValue", "000") //处理为空的数据
      .load(data_path_k)
      .select("_c0","_c1","_c2","_c3","_c4")
      .toDF("time_id","imsi","lac_id","cell_id","phone")
      .createTempView("traffic_table")

    val data_path_m = "file:///D:\\data\\基站经纬度数据.csv"
    //读取静态数据
    spark.read.format("csv")
      .option("delimiter", ",") //分隔符
      .option("qoute", "")
      .option("nullValue", "000")
      .load(data_path_m)
      .select("_c0","_c1","_c2")
      .toDF("longitude","latitude","laci")
      .createTempView("static_table")


    val data_path_r="file:///D:\\data\\原始数据.csv"
    //读取通信数据
    spark.read.format("csv")
      .option("delimiter", ",") //分隔符
      .option("qoute", "")
      .option("nullValue", "000") //处理为空的数据
      .load(data_path_r)
      .select("_c0","_c1","_c2","_c3","_c4")
      .toDF("time_id","imsi","lac_id","cell_id","phone")
      .createTempView("traffic_table")

    //数据预处理
    spark.sql(
      """
        |select * from traffic_table
        |where
        |imsi != '000'
        |and imsi not like  '%#%'
        |and imsi not like '%*%'
        |and imsi not like '%^%'
        |and lac_id != '000'
        |and cell_id != '000'
        |
      """.stripMargin).createTempView("traffic_table_end")


    // 数据抽取、时间戳转换格式、去除经纬度为空
    spark.sql(
      """
        |
        |select
        |   from_unixtime(substr(tt.time_id,0,10),'yyyyMMddHHmmss') as time_id,
        |   tt.imsi,
        |   st.longitude,
        |   st.latitude,
        |   tt.lac_id,
        |   tt.cell_id
        | from traffic_table_end tt
        | join static_table st
        | on concat(tt.lac_id,'-',tt.cell_id) = st.laci
        | where
        | st.longitude != '000'
        | and st.latitude !='000'
        |
      """.stripMargin)
      .distinct()
      .createTempView("result")

    //去除干扰数据条目、按时间正序排序
    val data = spark.sql(
      """
        |
        |select
        |  time_id,
        |  imsi,
        |  longitude,
        |  latitude,
        |  lac_id,
        |  cell_id,
        |  row_number() over (partition by imsi order by time_id) number
        |  from  result
        |  where
        |  time_id > '20181003000000'
        |
    """.stripMargin)

    data.createTempView("data_table")
    ///////改////////
    val imsi20 = spark.sql(
      """
        |select dt.imsi from (
        |   select imsi,count(1) count
        |  from data_table
        |group by imsi ) dt
        | where dt.count<20
      """.stripMargin)

    imsi20.createTempView("imsi20_table")

    ///////改////////

    imsi20.show()

    val result = spark.sql(
      """
        |select
        |   dt.time_id,
        |   dt.imsi,
        |   dt.longitude,
        |   dt.latitude,
        |   dt.lac_id,
        |   dt.cell_id,
        |   dt.number
        |  from
        |data_table dt
        |left join imsi20_table it
        |on dt.imsi = it.imsi
        | where
        |  it.imsi is null
        |
      """.stripMargin)


    print("數據條目："+result.count())

    val result_path = "src/outputpath"
    result.coalesce(1).write.csv(result_path)

  }

}
