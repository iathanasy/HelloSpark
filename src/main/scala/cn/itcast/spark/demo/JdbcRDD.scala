package cn.itcast.spark.demo

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JdbcRDD").setMaster("local[2]")
    val sc = new SparkContext(conf)
    /**
      * 获取mysql jdbc连接
      * 匿名函数 不传参
      */
    val conn = () => {
      Class.forName("com.mysql.jdbc.Driver").newInstance()
      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","zhongjilian")
    }

    val sql = "SELECT * FROM location_info where id >= ? AND id <= ?"

    val jdbcRDD = new JdbcRDD(
      sc,
      conn,
      sql,
      //开始id
      6,
      //结束 id
      10,
      //多少个numPartitions
      2,
      //返回结果
      r => {
        val id = r.getInt(1)
        val location = r.getString(2)
        val counts = r.getInt(3)
        val accesse_date = r.getDate(4)
        (id, location, counts, accesse_date)
      }
    )

    println(jdbcRDD.collect().toBuffer)
    sc.stop()
  }
}
