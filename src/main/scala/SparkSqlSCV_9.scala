import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import com.alibaba.fastjson.JSON
import scala.collection.mutable
import org.apache.spark.sql.functions.col

object SparkSqlCSV_9 {

  def main(args: Array[String]): Unit = {
    //1.创建Spark环境配置对象
    val conf = new SparkConf().setAppName("SparkSqlMovie").setMaster("local")

    //2.创建SparkSession对象
    val spark:SparkSession = SparkSession.builder().config(conf).getOrCreate()
    import spark.implicits._
    case class Accumulator(
                            result: Seq[String] = Nil,
                            openBracket: Boolean = false,
                            buffer: String = "",
                            previousIsHyphen: Boolean = false
                          ) {
      def flush(): Seq[String] = (buffer +: result).reverse
    }
    def splitRow(row: String): Seq[String] = row.foldLeft(Accumulator())((acc, char) => char match {
      case ',' if !acc.openBracket => acc.copy(result = acc.buffer +: acc.result, buffer = "", previousIsHyphen = false)
      case '[' if acc.previousIsHyphen => acc.copy(buffer = "[", openBracket = true, previousIsHyphen = false)
      case '"' if !acc.openBracket => acc.copy(previousIsHyphen = true)
      case '"' if acc.buffer.last == ']' => acc.copy(openBracket = false)
      case _ => acc.copy(buffer = acc.buffer + char, previousIsHyphen = false)
    }).flush()

    val commentData = spark.read.text("src\\input\\keywords.csv")
      .as[String]
      .map(splitRow)
      .withColumn("id", col("value").getItem(0).cast("int"))
      .withColumn("keywords", col("value").getItem(1).cast("string"))


    //3.注册临时表
    commentData.createOrReplaceTempView("tbl_keywords")

    //4.查询操作
    val sqlresult_words :DataFrame = spark.sql("select keywords from tbl_keywords ")
    val array = sqlresult_words.collect
    val map_num = mutable.Map(("Paris",0))

    sqlresult_words.show()


    for(i <- 0 until array.length){
      val jsonArray = array(i)(0).toString.replaceAll("\"\"","\"")
      val parseJsonArray = JSON.parseArray(jsonArray)
      //遍历
      for (k<- 0 until parseJsonArray.size()){
        val jsonObject = parseJsonArray.getJSONObject(k)
        val id = jsonObject.getInteger("id")
        val name = jsonObject.getString("name")
        println("(k,v)：" + name + "===" + id)
        if(map_num.contains(name)) map_num(name) = map_num(name)+1
        else map_num+=(name->1)
      }
    }


    for ((k, v) <- map_num) {
      println("(k,v)：" + k + "===" + v)
    }

    val df1 = map_num.toSeq.toDF("keyword", "num")
    df1.createOrReplaceTempView("tbl_keyword_num")



    df1.show(40000)


    //5.将分析结果保存到数据表中
    df1.write
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/sparkdb")
      .option("user","root")
      .option("password","123456" )
      .option("dbtable","movies_keywords")
      .mode(SaveMode.Append)
      .save()
  }
}