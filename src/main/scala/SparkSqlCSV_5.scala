package SparkOnHDFS

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSqlCSV_3 {

<<<<<<<< HEAD:src/main/scala/SparkSqlCSV_5.scala
object SparkSqlCSV_5 {
========
>>>>>>>> a8340a4 (11):src/main/scala/SparkSqlCSV_3.scala
  def main(args: Array[String]): Unit = {
    //1.创建Spark环境配置对象
    val conf = new SparkConf().setAppName("SparkSqlMovie").setMaster("local")

    //2.创建SparkSession对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    var commentData: DataFrame = spark.read.format("csv")
      .option("header", true)
      .option("multiLine", true)
      .load("src\\input\\movies_metadata.csv")
    commentData.show()

    //3.注册临时表
    commentData.createOrReplaceTempView("tbl_movies")

    //4.查询操作
    val sqlresult_type: DataFrame =
      spark.sql("select genres,release_date from tbl_movies ")
    val array = sqlresult_type.collect
<<<<<<<< HEAD:src/main/scala/SparkSqlCSV_5.scala
    val map_name = mutable.Map(("Fantasy", 0:Long))

    val regex="""^\d+$""".r
    def IsNumber(str: String) = {
      var flag = true
      for (i <- 0 until str.length) {
        if ("0123456789".indexOf(str.charAt(i)) < 0) flag = false
      }
      flag
    }

    for (i <- 0 to array.length - 1) {
      val jsonArray = array(i)(0).toString //genres
      val parseJsonArray = JSON.parseArray(jsonArray)
      val revenue0 = array(i)(1).toString
      if (IsNumber(revenue0)) {
        val revenue = array(i)(1).toString.toLong
        //遍历
        for (i <- 0 until parseJsonArray.size) {
          val jsonObject = parseJsonArray.getJSONObject(i)
          val name = jsonObject.getString("name")

          //票房统计
          if (map_name.contains(name)) map_name(name) = map_name(name) + revenue
          else map_name += (name -> revenue)
        }
      }
    }

    for ((k, v) <- map_name) {
      println("(k,v)：" + k + "===" + v)
    }

    val df1 = map_name.toSeq.toDF("type", "revenue")
    df1.createOrReplaceTempView("tbl_type_revenue")
    df1.show(50)
========
    var map_T_T:Map[String,Map[String,Int]] = Map()

    for(i <- 0 to array.length-1) {
      val jsonArray = array(i)(0).toString //类型
      val parseJsonArray = JSON.parseArray(jsonArray)
      val Date = array(i)(1).toString.split("/")//时间
      val year = Date(0)
      for (i <- 0 until parseJsonArray.size) {
        val jsonObject = parseJsonArray.getJSONObject(i)
        val name = jsonObject.getString("name")//类型
        if(map_T_T.contains(name)){
          val map1 = map_T_T(name)
          if(map1.contains(year)) {
            map1(year) = map1(year)+1
            map_T_T = map_T_T.updated(name,map1)//都有
          } else
            map_T_T = map_T_T.updated(name,Map(year -> 1))//新建年份
        }
        else
          map_T_T += (name,Map())//新建类型
      }
    }

    val df1 = map_T_T.toSeq.toDF("type", "year","num")
    df1.createOrReplaceTempView("tbl_type_time")
    df1.show()
>>>>>>>> a8340a4 (11):src/main/scala/SparkSqlCSV_3.scala

    //5.将分析结果保存到数据表中
    df1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("user", "root")
      .option("password", "123456")
<<<<<<<< HEAD:src/main/scala/SparkSqlCSV_5.scala
      .option("dbtable", "movies_type_revenue")
========
      .option("dbtable", "tbl_movies_type_time")
>>>>>>>> a8340a4 (11):src/main/scala/SparkSqlCSV_3.scala
      .mode(SaveMode.Append)
      .save()
  }
}
