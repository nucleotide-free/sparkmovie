import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.:+
import scala.collection.mutable

case class MovieTT(MType: String, MTime: String, count: Int)

object SparkSqlCSV_3 {
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
    val MovieTTList = mutable.MutableList[MovieTT]()

    for(i <- 0 to array.length-1) {
      val jsonArray = array(i)(0).toString //类型
      val parseJsonArray = JSON.parseArray(jsonArray)
      val Date = array(i)(1).toString.split("/")//时间
      val year = Date(0)
      for (j <- 0 until parseJsonArray.size) {
        val jsonObject = parseJsonArray.getJSONObject(j)
        val name = jsonObject.getString("name")//类型

        var flag = 0
        for (k <- 0 to MovieTTList.length -1;if flag == 0) {
          val elem = MovieTTList(k)
          if(elem.MType == name && elem.MTime == year){
            MovieTTList(k) = MovieTT(elem.MType,elem.MTime,elem.count+1)
            flag = 1
          }
        }
        if(flag == 0) { //没找到
          MovieTTList += MovieTT(name, year, 1)
        }
      }
    }

    val df1 = MovieTTList.toSeq.toDF("type", "year","num")
    df1.createOrReplaceTempView("tbl_type_time")
    df1.show()

    //5.将分析结果保存到数据表中
    df1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("user", "root")
      .option("password", "100708007sM")
      .option("dbtable", "movies_type_time_num")
      .mode(SaveMode.Append)
      .save()
  }
}