package cn.itcast.spark.demo

import java.net.URL

import org.apache.spark.{HashPartitioner, Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 区分数据
  */
object UrlCountPartitionDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCountPartitionDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //rdd1将数据切分，元组中放的是（URL， 1）
    val rdd1 = sc.textFile("D:\\files\\usercount\\itcast.log").map(line =>{
      val f = line.split("\t")
      (f(1), 1)
    })
    //统计url条数
    val rdd2 = rdd1.reduceByKey(_ + _)
    //取出host 返回数据 (php.itcast.cn,(http://php.itcast.cn/php/course.shtml,459))
    val rdd3 = rdd2.map(t =>{
      val url = t._1
      val host = new URL(url).getHost
      (host, (url, t._2))
    })
    //取出 host distinct()去重 collect 立即执行action
    val ins = rdd3.map(_._1).distinct().collect()
    /**
      * 数据没有分区
      * (net.itcast.cn,(http://net.itcast.cn/net/teacher.shtml,512))
      * (java.itcast.cn,(http://java.itcast.cn/java/course/hadoop.shtml,506))
      */
    /*val rdd4 = rdd3.partitionBy(new HashPartitioner(ins.length)).sortBy(_._2._2,false)
    rdd4.saveAsTextFile("D:\\files\\out1")*/

    val hostPartitioner = new HostPartitioner(ins)

    val rdd4 = rdd3.partitionBy(hostPartitioner).mapPartitions(it => {
      it.toList.sortBy(_._2._2).iterator
    })
    rdd4.saveAsTextFile("D:\\files\\out2")
    println(rdd4.collect().toBuffer)
    sc.stop()
  }
}

/**
  * 决定了数据到那个分区里面
  * @param ins
  */
class HostPartitioner(ins: Array[String]) extends Partitioner{
  //
  val parMap = new mutable.HashMap[String,Int]()
  var count = 0
  for (i <- ins) {
    parMap += (i -> count)
    count += 1
  }
  println("parMap: " + parMap)
  //几个分区
  override def numPartitions = ins.length

  override def getPartition(key: Any) = {
    parMap.getOrElse(key.toString, 0)
  }
}