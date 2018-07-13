package cn.itcast.spark.demo

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import cn.itcast.spark.day3.IPLocation.data2MySQL
import org.apache.spark.{SparkConf, SparkContext}

object IPLocationDemo {
  /**
    * 插入mysql 数据库
    */
  val data2MySQL = (iterator: Iterator[(String, Int)]) => {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "INSERT INTO location_info (location, counts, accesse_date) VALUES (?, ?, ?)"
    try {
      //获取连接
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "zhongjilian")
      //2 个Partition
      println("----------mysql conn ------------")
      iterator.foreach(line => {
        //执行sql语句
        ps = conn.prepareStatement(sql)
        //插入参数
        ps.setString(1, line._1)
        ps.setInt(2, line._2)
        ps.setDate(3, new Date(System.currentTimeMillis()))
        ps.executeUpdate()
      })
    } catch {
      case e: Exception => println("MySql Exception")
    } finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }
  }

  /**
    * 将ip转换为 16进制数
    * @param ip
    * @return
    */
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分法查找 要求必须是有序序列，找出其中某一个元素
    * @param lines
    * @param ip
    * @return
    */
  def binarySearch(lines: Array[(String, String, String)], ip: Long) : Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IPLocationDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //加载规则库
    val ipRulesRdd = sc.textFile("D://files//ip.txt").map(line =>{
      val f = line.split("\\|")
      //16进制 ip地址
      val start_num = f(2)
      val end_num = f(3)
      //省份
      val province = f(6)
      val city = f(7)
      (start_num, end_num, province+"-"+city)
    })

    //全部ip映射规则
    val ipRulesArrary = ipRulesRdd.collect()

    //广播规则 广播给所有的work
    val ipRulesBroadcast = sc.broadcast(ipRulesArrary)

    //加载要处理的数据
    val ipsRDD = sc.textFile("D:\\files\\access_log").map(line =>{
      val f = line.split("\\|")
      //访问的ip地址
      f(1)
    })

    //处理数据
    val result = ipsRDD.map(ip =>{
      val ipNum = ip2Long(ip)
      val index = binarySearch(ipRulesBroadcast.value,ipNum)
      val info = ipRulesBroadcast.value(index)
      info
    }).map(t => (t._3, 1)).reduceByKey(_ + _)
    //存入数据库
    result.foreachPartition(data2MySQL)

    println(result.collect().toBuffer)
    sc.stop()
  }
}
