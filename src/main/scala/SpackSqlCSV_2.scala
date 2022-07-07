package SparkOnHDFS

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.alibaba.fastjson.JSON
import scala.collection.mutable

object SpackSqlCSV_2 {

  def main(args: Array[String]): Unit = {
    //1.创建Spark环境配置对象
    val conf = new SparkConf().setAppName("SparkSqlMovie").setMaster("local")
    //2.创建SparkSession对象
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()

    import spark.implicits._

    var commentData: DataFrame = spark.read.format("csv")
      .option("header", true)
      .option("multiLine", true)
      .load("src\\input\\movies_metadata.csv")
    commentData.show()

    //3.注册临时表
    commentData.createOrReplaceTempView("tbl_movies")
    //4.查询操作
    val sqlResult_year :DataFrame=
    spark.sql("select release_date,revenue from tbl_movies")
    sqlResult_year.show(45366)

    val array = sqlResult_year.collect

    var map_num = mutable.Map(("2020",0:Long))// Year MovieNum
    var map_revenue = mutable.Map(("2020",0:Long))// Year Revenue

    val regex="""^\d+$""".r
    def IsNumber(str: String) = {
      var flag = true
      for (i <- 0 until str.length) {
        if ("0123456789".indexOf(str.charAt(i)) < 0) flag = false
      }
      flag
    }
    for(i <- 0 to array.length-1) {
      if (array(i)(0) != null) {
        val Date = array(i)(0).toString.split("/")
        val year = Date(0).toString
        val revenue0 = array(i)(1).toString
        if (IsNumber(revenue0)) {
          val revenue = array(i)(1).toString.toLong
          //每年电影数量
          if (map_num.contains(year)) map_num(year) = map_num(year) + 1
          else map_num += (year -> 1)
          //票房统计
          if (map_revenue.contains(year)) map_revenue(year) = map_revenue(year) + revenue
          else map_revenue += (year -> revenue)
        }
      }
    }

    for ((k, v) <- map_num) {
      println("(k,v)：" + k + "===" + v)
    }



    val df1 = map_num.toSeq.toDF("year", "num")
    df1.createOrReplaceTempView("tbl_year_num")
    df1.show(160)
    val df2 = map_revenue.toSeq.toDF("year", "revenue")
    df2.createOrReplaceTempView("tbl_year_revenue")
    df2.show(160)

    val sqlresult_year :DataFrame=
      spark.sql( "SELECT tbl_year_revenue.year,num,revenue " +
        "FROM tbl_year_num JOIN tbl_year_revenue " +
        "ON tbl_year_num.year = tbl_year_revenue.year")
    //sqlresult_year.show()

    //5.将分析结果保存到数据表中
    sqlresult_year.write
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/sparkdb")
      .option("user","root")
      .option("password","100708007sM" )
      .option("dbtable","movies_years")
      .mode(SaveMode.Append)
      .save()
  }

}
