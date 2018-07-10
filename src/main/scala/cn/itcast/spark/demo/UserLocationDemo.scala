package cn.itcast.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 根据日志统计出每个用户在站点所呆时间最长的前2个的信息
  *   1, 先根据"手机号_站点"为唯一标识, 算一次进站出站的时间, 返回(手机号_站点, 时间间隔)
  *   2, 以"手机号_站点"为key, 统计每个站点的时间总和, ("手机号_站点", 时间总和)
  *   3, ("手机号_站点", 时间总和) --> (手机号, 站点, 时间总和)
  *   4, (手机号, 站点, 时间总和) --> groupBy().mapValues(以时间排序,取出前2个) --> (手机->((m,s,t)(m,s,t)))
  */
object UserLocationDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UserLocationDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val mbt = sc.textFile("D://files//userlocal").map(line =>{
      val fiedles = line.split(",")
      val eventType = fiedles(3)
      val time = fiedles(1)
      val timeLong = if(eventType.equals("1")) -time.toLong else time.toLong
      //返回 (18688888888_16030401EAFB68F1E3CDF819735E1C66,20160327082400)
      (fiedles(0) + "_" + fiedles(2),timeLong)
    })
   //时间相减 (18611132889_9F36407EAD0629FC166F14DDE7970F68,54000)
    val rdd1 = mbt.groupBy(_._1).mapValues(_.foldLeft(0L)(_ + _._2 ))
    //(18611132889,9F36407EAD0629FC166F14DDE7970F68,54000)
    val rdd2 = rdd1.map(t =>{
      val mobile_bs = t._1
      val mobile = mobile_bs.split("_")(0)
      val lac = mobile_bs.split("_")(1)
      val time = t._2
      (mobile,lac,time)
    })
    //根据手机号分组
    val rdd3 = rdd2.groupBy(_._1)
    //根据时间长短排序 取前面的2组数据
    val rdd4 = rdd3.mapValues(it =>{
      it.toList.sortBy(_._3).reverse.take(2)
    })
    println(rdd1.collect().toBuffer)
    sc.stop()
  }
}
