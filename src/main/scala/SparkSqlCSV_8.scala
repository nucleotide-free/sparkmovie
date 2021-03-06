import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import scala.collection.mutable

case class MovieTV(MTime: Int, MVote: Double)

object SparkSqlCSV_8 {
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
      spark.sql("select runtime,vote_average from tbl_movies ")
    val array = sqlresult_type.collect
    val TVList = mutable.MutableList[MovieTV]()

    val regex = """^\d+$""".r

    def IsNumber(str: String) = {
      var flag = true
      for (i <- 0 until str.length) {
        if ("0123456789.".indexOf(str.charAt(i)) < 0) flag = false
      }
      flag
    }

    for (i <- 0 to array.length - 1) {
      if (array(i)(0) != null) {
        val time = array(i)(0).toString.toInt
        if(time>0){
          val vote_average0 = array(i)(1).toString
          if (IsNumber(vote_average0)) {
            val vote_average = array(i)(1).toString.toDouble
            TVList += MovieTV(time,vote_average)
          }
        }
      }
    }


    val df1 = TVList.toDF("time", "vote")
    df1.createOrReplaceTempView("tbl_time_vote")
    df1.show(40000)

    //5.将分析结果保存到数据表中
    df1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "movies_time_vote")
      .mode(SaveMode.Append)
      .save()
  }
}
