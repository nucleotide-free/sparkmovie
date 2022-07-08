import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.alibaba.fastjson.JSON
import scala.collection.mutable

object SparkSqlCSV_9 {

  def main(args: Array[String]): Unit = {
    //1.创建Spark环境配置对象
    val conf = new SparkConf().setAppName("SparkSqlMovie").setMaster("local")

    //2.创建SparkSession对象
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    var commentData: DataFrame = spark.read.format("csv")
      .option("header", true)
      .option("multiLine", true)
      .load("src\\input\\keywords.csv")
    commentData.show()

    //3.注册临时表
    commentData.createOrReplaceTempView("tbl_keywords")

    //4.查询操作
    val sqlresult_words :DataFrame = spark.sql("select keywords from tbl_keywords ")
    val array = sqlresult_words.collect
    val map_num = mutable.Map(("Paris",0))


    for(i <- 0 to array.length-1){
      for(j <- 0 to array(i).length-1){
        val jsonArray = array(i)(j).toString
        val parseJsonArray = JSON.parseArray(jsonArray)
        //遍历
        for (k<- 0 until parseJsonArray.size){
          val jsonObject = parseJsonArray.getJSONObject(k)
          val id = jsonObject.getInteger("id")
          val name = jsonObject.getString("name")

          if(map_num.contains(name)) map_num(name) = map_num(name)+1
          else map_num+=(name->1)

        }
      }
    }


    for ((k, v) <- map_num) {
      println("(k,v)：" + k + "===" + v)
    }

    val df1 = map_num.toSeq.toDF("keyword", "num")
    df1.createOrReplaceTempView("tbl_keyword_num")


//    val sqlresult_type_num :DataFrame=
//      spark.sql( "select tbl_type_name.name,id,num" +
//        "from tbl_type_num  join tbl_type_name" +
//        "on tbl_type_num.name=tbl_type_name.name")
    df1.show()
//    sqlresult_type_num.show()

    //5.将分析结果保存到数据表中
    df1.write
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/sparkdb")
      .option("user","root")
      .option("password","100708007sM" )
      .option("dbtable","movies_keywords")
      .mode(SaveMode.Append)
      .save()
  }
}