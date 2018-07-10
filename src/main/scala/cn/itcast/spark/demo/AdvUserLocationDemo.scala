package cn.itcast.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

object AdvUserLocationDemo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdvUserLocationDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val mbt = sc.textFile("D://files//userlocal").map(line =>{
      val fields = line.split(",")
      val eventType = fields(3)
      val time = fields(1)
      val timeLong = if(eventType == "1") -time.toLong else time.toLong
      //返回数据
      ((fields(0),fields(2)), timeLong)
    })

    // (CC0710CC94ECC657A8561DE549D940E0,(18688888888,1300))
    val rdd1 = mbt.reduceByKey(_+_).map(t =>{
     //((18688888888,CC0710CC94ECC657A8561DE549D940E0),1300)
      val mobile = t._1._1
      val lac = t._1._2
      val time = t._2
      (lac, (mobile, time))
    })
    val rdd2 = sc.textFile("D://files/loc_info.txt").map(line =>{
      val f= line.split(",")
      // (基站ID ，经度，纬度)
      (f(0), (f(1),f(2)))
    })
    // rdd1.join(rdd2) (CC0710CC94ECC657A8561DE549D940E0,((18688888888,1300),(116.303955,40.041935)))
    val rdd3 = rdd1.join(rdd2).map(t =>{
      val lac = t._1
      val mobile = t._2._1._1
      val time = t._2._1._2
      val x = t._2._2._1
      val y = t._2._2._2
      (mobile, lac, time, x, y)
    })
    //根据手机号分组 (18688888888,CompactBuffer((18688888888,CC0710CC94ECC657A8561DE549D940E0,1300,116.303955,40.041935)
    val rdd4 = rdd3.groupBy(_._1)
    //获取map中的values 排序并只返回前三
    val rdd5 = rdd4.mapValues(it =>{
      it.toList.sortBy(_._3).reverse.take(3)
    })
    //数据输出到磁盘
    rdd5.saveAsTextFile("D://files/out")
    println(rdd5.collect.toBuffer)
    sc.stop()
  }
}
