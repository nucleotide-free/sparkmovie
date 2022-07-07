import net.minidev.json.{JSONArray, JSONObject}
import net.minidev.json.parser.JSONParser
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.alibaba.fastjson.JSON
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.xml.XML.parser

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
    val sqlresult_type :DataFrame=
      spark.sql("select genres " +
        "   from tbl_movies ")
    var array = sqlresult_type.collect
    val map_num=mutable.Map(("Fantasy",0))
    val map_name=mutable.Map(("Fantasy",14))
    val parser = new JSONParser(JSONParser.BIG_DIGIT_UNRESTRICTED)
    for(i <- 0 to array.length-1){
      for(j <- 0 to array(i).length-1){
        val jsonArray = array(i)(j).toString
        val parseJsonArray = JSON.parseArray(jsonArray)
        //遍历
        for (i <- 0 until parseJsonArray.size){
          val jsonObject = parseJsonArray.getJSONObject(i)
          val id = jsonObject.getInteger("id")
          val name = jsonObject.getString("name")
          val bs1 =
            if(map_num.contains(name)) map_num(name)=map_num(name)+1
            else map_num+=(name->1)
          map_name+=(name->id)
          //println(s"$id is $name")
        }
      }
    }

    for ((k, v) <- map_num) {
      println("(k,v)：" + k + "===" + v)
    }

    val df1 = map_num.toSeq.toDF("name", "num")
    val df2 = map_name.toSeq.toDF("name", "id")

    df1.createOrReplaceTempView("tbl_type_num")
    df2.createOrReplaceTempView("tbl_type_name")

    val sqlresult_type_num :DataFrame=
      spark.sql( "select tbl_type_name.name,id,num from tbl_type_num  join tbl_type_name on tbl_type_num.name=tbl_type_name.name")
    df1.show()
    df2.show()
    sqlresult_type_num.show()



    //5.将分析结果保存到数据表中
    //       eg:存入oa数据库中
    sqlresult_type_num.write
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/sparkdb")
      .option("user","root")
      .option("password","100708007sM" )
      .option("dbtable","tbl_movies_process_result")
      .mode(SaveMode.Append)
      .save()
  }
}
