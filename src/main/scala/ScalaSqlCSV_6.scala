import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.alibaba.fastjson.JSON
import scala.collection.mutable

object ScalaSqlCSV_6 {
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
      spark.sql("select genres,vote_average from tbl_movies ")
    val array = sqlresult_type.collect
    val map_num = mutable.Map(("Fantasy",0))//typenum
    val map_name = mutable.Map(("Fantasy", 0.00))


    for (i <- 0 to array.length - 1) {
      val jsonArray = array(i)(0).toString//genres
      val parseJsonArray = JSON.parseArray(jsonArray)
      val vote_average = array(i)(1).toString.toDouble//revenue

      //遍历
      for (i <- 0 until parseJsonArray.size) {
        val jsonObject = parseJsonArray.getJSONObject(i)
        val name = jsonObject.getString("name")
        //类型统计
        if(map_num.contains(name)) map_num(name)=map_num(name)+1
        else map_num+=(name->1)
        //票房统计
        if (map_name.contains(name)) map_name(name) = map_name(name) + vote_average
        else map_name += (name -> vote_average)
      }
    }

    for((k, v) <- map_name) {
      for ((a, b) <- map_num) {
        if(k == a)
          map_name(k) = map_name(k)/b
      }
      println("(k,v)：" + k + "===" + v)
    }

    val df1 = map_name.toSeq.toDF("type", "vote")
    df1.createOrReplaceTempView("tbl_type_vote")
    df1.show()

    //5.将分析结果保存到数据表中
    df1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "tbl_movies_type_vote")
      .mode(SaveMode.Append)
      .save()
  }
}
