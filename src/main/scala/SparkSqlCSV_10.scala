import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.:+
import scala.collection.mutable

case class MovieCC(country: String, company: String, count: Int)
object SparkSqlCSV_10 {
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
        spark.sql("select production_countries,production_companies from tbl_movies ")
      val array = sqlresult_type.collect
      val MovieCCList = mutable.MutableList[MovieCC]()

      for(i <- 0 to array.length-1) {
        val jsonArray = array(i)(0).toString //类型
        val parseJsonArray = JSON.parseArray(jsonArray)
        if (parseJsonArray.size() > 0) {
          val jsonObject = parseJsonArray.getJSONObject(0)
          val country_name = jsonObject.getString("name") //类型
          val jsonArray2 = array(i)(1).toString //类型
          val parseJsonArray2 = JSON.parseArray(jsonArray2)
          if (parseJsonArray2.size() > 0) {
            val jsonObject2 = parseJsonArray2.getJSONObject(0)
            val company_name = jsonObject2.getString("name") //类型
            println("(k,v)：" + country_name + "===" + company_name)
            var flag = 0
            for (k <- 0 to MovieCCList.length - 1; if flag == 0) {
              val elem = MovieCCList(k)
              if (elem.country == country_name && elem.company == company_name) {
                MovieCCList(k) = MovieCC(elem.country, elem.company, elem.count + 1)
                flag = 1
              }
            }
            if (flag == 0) { //没找到
              MovieCCList += MovieCC(country_name, company_name, 1)
            }

          }
        }
      }

      val df1 = MovieCCList.toSeq.toDF("country", "company","num")
      df1.createOrReplaceTempView("tbl_type_time")
      df1.show()

      //5.将分析结果保存到数据表中
      df1.write
        .format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/sparkdb")
        .option("user", "root")
        .option("password", "100708007sM")
        .option("dbtable", "movies_country_company")
        .mode(SaveMode.Append)
        .save()
    }
}
