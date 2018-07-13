package cn.itcast.spark.day3

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ZX on 2016/4/12.
  */
object JdbcRDDDemo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("JdbcRDDDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val connection = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "zhongjilian")
    }
    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM location_info where id >= ? AND id <= ?",
      1, 5, 2,
      r => {
        val id = r.getInt(1)
        val location = r.getString(2)
        val counts = r.getInt(3)
        val accesse_date = r.getDate(4)
        (id, location, counts, accesse_date)
      }
    )
    val jrdd = jdbcRDD.collect()
    println(jdbcRDD.collect().toBuffer)
    sc.stop()
  }
}
