package cn.itcast.spark.demo

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据指定学科 取出点击量前三的
  */
object AdvUrlCountDemo {
  //从数据库中 加载规则
  val arr = Array("java.itcast.cn", "php.itcast.cn", "net.itcast.cn")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdvUrlCountDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D://files//usercount//itcast.log").map(t =>{
       val f = t.split("\t")
       (f(1), 1)
     })
    //计数
    val rdd1 = rdd.reduceByKey(_ + _)
    //取出 host
    val rdd2 = rdd1.map(t =>{
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2)
    })

    for(ins <- arr){
      //匹配规则
      val rdd3 = rdd2.filter(_._1 == ins)
      val rdd4 = rdd3.sortBy(_._3,false).take(3)
      //通过JDBC向数据库中存储数据
      //id，学院，URL，次数， 访问日期
      println(rdd4.toBuffer)
    }


    //println(rdd1.collect().toBuffer)
    sc.stop()
  }
}
