package cn.itcast.spark.demo

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 取出学科点击前三的 Url
  */
object UrlCountDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCountDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("D://files//usercount//itcast.log").map(line =>{
      val fields = line.split("\t")
      // url - 1
      (fields(1), 1)
    })
    //计数
    val rdd1 = rdd.reduceByKey(_ + _)
    //url host (php.itcast.cn,http://php.itcast.cn/php/course.shtml,459)
    val rdd2 = rdd1.map(t =>{
      val url = t._1
      val host = new URL(url).getHost
      (host, url, t._2)
    })
    val rdd3 = rdd2.groupBy(_._1).mapValues(t =>{
      //(net.itcast.cn,CompactBuffer((net.itcast.cn,http://net.itcast.cn/net/video.shtml,521)
      t.toList.sortBy(_._3).reverse.take(3)
    })
    println(rdd3.collect().toBuffer)
    sc.stop()
  }
}
