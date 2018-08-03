package cn.itcast.spark.day6

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


/**
  * @Auther: cd
  * @Date: 2018/8/2 10:30
  * @Description:
  */
object TextSearch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TextSearch").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val text = sc.textFile("F:\\all.txt")

    import sqlContext.implicits._
    val df = text.toDF("line")
    //val h = df.filter(df.col("line").like("%韩立%"))
    df.registerTempTable("hanli")
    val h =sqlContext.sql("select * from hanli where line like '%天南%'")
    println(h.count())
    h.show()
    val d = h.filter(df.col("line").like("%道友%"))
    d.show()
    d.collect()
    h.write.text("F:\\write\\hanli")
    d.write.text("f:\\write\\daoyou")
    sc.stop()
  }
}
