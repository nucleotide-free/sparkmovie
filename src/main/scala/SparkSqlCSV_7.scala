import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import scala.collection.mutable

case class MovieTR(MTime: Int, MRevenue: Long)

object SparkSqlCSV_7 {
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
    val sqlresult_type :DataFrame=
    spark.sql("select runtime,revenue from tbl_movies ")
    val array = sqlresult_type.collect
    val TRList = mutable.MutableList[MovieTR]()

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
        val time = array(i)(0).toString.toInt
        if(time>0){
          val revenue0 = array(i)(1).toString
          if (IsNumber(revenue0)) {
            val revenue = array(i)(1).toString.toLong
            //票房统计
            TRList += MovieTR(time,revenue)
          }
        }
      }
    }


    val df1 = TRList.toDF("time", "revenue")
    df1.createOrReplaceTempView("tbl_time_revenue")
    df1.show(40000)

    //5.将分析结果保存到数据表中
    df1.write
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/sparkdb")
      .option("user","root")
      .option("password","123456" )
      .option("dbtable","movies_time_revenue")
      .mode(SaveMode.Append)
      .save()
  }
}
