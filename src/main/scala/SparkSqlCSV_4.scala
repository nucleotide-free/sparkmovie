import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.alibaba.fastjson.JSON
import scala.collection.mutable

object SparkSqlCSV_4 {
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
      spark.sql("select production_countries from tbl_movies ")
    val array = sqlresult_type.collect
    val map_area = mutable.Map(("CN", 0))
    val map_name = mutable.Map(("CN", "China"))

    for(i <- 0 to array.length-1){
      for(j <- 0 to array(i).length-1){
        val jsonArray = array(i)(j).toString
        val parseJsonArray = JSON.parseArray(jsonArray)
        //遍历
        for (i <- 0 until parseJsonArray.size){
          val jsonObject = parseJsonArray.getJSONObject(i)
          val short = jsonObject.getString("iso_3166_1")
          val name = jsonObject.getString("name")

          if(map_area.contains(short)) map_area(short)=map_area(short)+1
          else map_area+=(short->1)

          map_name += (short->name)
        }
      }
    }

    for ((k, v) <- map_area) {
      println("(k,v)：" + k + "===" + v)
    }

    val df1 = map_area.toSeq.toDF("short", "num")
    df1.createOrReplaceTempView("tbl_short_num")

    val df2 = map_name.toSeq.toDF("short", "name")
    df2.createOrReplaceTempView("tbl_short_name")

    val sqlresult_area :DataFrame=
      spark.sql( "SELECT tbl_short_num.short,name,num " +
        "FROM tbl_short_num JOIN tbl_short_name " +
        "ON tbl_short_num.short = tbl_short_name.short")
    df1.show()
    df2.show()
    sqlresult_area.show()

    //5.将分析结果保存到数据表中
    df1.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "movies_area_num")
      .mode(SaveMode.Append)
      .save()
  }
}

