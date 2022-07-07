//import com.alibaba.fastjson.JSON
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
//
//object SparkSqlCSV_3 {
//  def main(args: Array[String]): Unit = {
//    //1.创建Spark环境配置对象
//    val conf = new SparkConf().setAppName("SparkSqlMovie").setMaster("local")
//
//    //2.创建SparkSession对象
//    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
//    import spark.implicits._
//    var commentData: DataFrame = spark.read.format("csv")
//      .option("header", true)
//      .option("multiLine", true)
//      .load("src\\input\\movies_metadata.csv")
//    commentData.show()
//
//    //3.注册临时表
//    commentData.createOrReplaceTempView("tbl_movies")
//
//    //4.查询操作
//    val sqlresult_type: DataFrame =
//      spark.sql("select genres,release_date from tbl_movies ")
//    val array = sqlresult_type.collect
//    var map_T_T:Map[String,Map[String,Int]] = Map()
//
//    for(i <- 0 to array.length-1) {
//      val jsonArray = array(i)(0).toString //类型
//      val parseJsonArray = JSON.parseArray(jsonArray)
//      val Date = array(i)(1).toString.split("/")//时间
//      val year = Date(0)
//      for (i <- 0 until parseJsonArray.size) {
//        val jsonObject = parseJsonArray.getJSONObject(i)
//        val name = jsonObject.getString("name")//类型
//        if(map_T_T.contains(name)){
//          val map1 = map_T_T(name)
//          if(map1.contains(year)) {
//            map1(year) = map1(year)+1
//            map_T_T = map_T_T.updated(name,map1)//都有
//          } else
//            map_T_T = map_T_T.updated(name,Map(year -> 1))//新建年份
//        }
//        else
//          map_T_T += (name,Map())//新建类型
//      }
//    }
//
//    val df1 = map_T_T.toSeq.toDF("type", "year","num")
//    df1.createOrReplaceTempView("tbl_type_time")
//    df1.show()
//
//    //5.将分析结果保存到数据表中
//    df1.write
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/sparkdb")
//      .option("user", "root")
//      .option("password", "123456")
//      .option("dbtable", "movies_type_time_num")
//      .mode(SaveMode.Append)
//      .save()
//  }
//}
//
