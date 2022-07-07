import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object ScalaSqlCSV_8 {
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

    //5.将分析结果保存到数据表中
    sqlresult_type.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "tbl_movies_time_vote")
      .mode(SaveMode.Append)
      .save()
  }
}
