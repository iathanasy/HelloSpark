package cn.itcast.spark.demo

import org.apache.spark.{SparkConf, SparkContext}

/**+
  * 隐式转换
  */
object OrderContext{
  //实现排序
  implicit val girlOrdering = new Ordering[Girl]{
    override def compare(x: Girl, y: Girl) = {
      if(x.faceValue > y.faceValue) 1
      else if (x.faceValue == y.faceValue)
        if(x.age > y.age) -1 else 1
      else -1
    }
  }
}

/**
  * 自定义排序
  */
object CustomSortDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSortDemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //生成数据
    val rdd1 = sc.parallelize(List(("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2),("JuJingYi", 95, 22, 3),("Hanmeimei", 90, 23, 4)))
    //隐式转换  需要导入
    import OrderContext.girlOrdering
    val rdd2 = rdd1.sortBy(x => Girl(x._2,x._3),false)
    println(rdd2.collect().toBuffer)
  }
}

/**
  * 第一种方式
  * @param faceValue
  * @param age
  */
/*
case class Girl(faceValue: Int, age: Int) extends Ordered[Girl] with Serializable{
  //自定义排序规则
  override def compare(that: Girl) = {
    println("this faceValue :" + faceValue + ", age :"+ age)
    println("that :" + that)
    // that ("yuihatano", 90, 28, 1), ("angelababy", 90, 27, 2),("JuJingYi", 95, 22, 3)
    if(this.faceValue == that.faceValue){
      println("if :" + (that.age - this.age))
      that.age - this.age
    }else{
      println("else: " + (this.faceValue - that.faceValue))
      this.faceValue - that.faceValue
    }
  }
}*/

/**
  * 第二种方式 通过隐式转换完成排序
  * @param faceValue
  * @param age
  */
case class Girl(faceValue: Int, age: Int) extends Serializable