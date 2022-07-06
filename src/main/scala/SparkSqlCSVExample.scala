import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}

/**
 * 知识点:
 *   SparkSQL:
 *   1)加载数据 (csv,jdbc,json,txt....)
 *     spark.read.
 *           format(" "): 加载的数据类型 csv,jdbc,json
 *           option(".."): //如果读取jdbc数据源，设置jdbc相应的参数driver,url,user,password,dbtable
 *           load("") :// 如果是csv,json文件，设定加载文件的路径
 *
 *   Spark sql读取csv文件
 *         src\\input\\comments.csv文件
 *
 *   虚拟机运行命令:
 *   1. ./bin/spark-submit --class SparkSqlCSVExample --master local sparksqlexample-1.0-SNAPSHOT.jar
 *   2.
 */
object SparkSqlCSVExample {


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
    val sqlresult : DataFrame=
      spark.sql("select * " +
        "   from tbl_movies ")

    sqlresult.show()
    //5.将分析结果保存到数据表中
    //       eg:存入oa数据库中
    sqlresult.write
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/sparkdb")
      .option("user","root")
      .option("password","100708007sM" )
      .option("dbtable","tbl_movies_process_result")
      .mode(SaveMode.Append)
      .save()
  }
}
