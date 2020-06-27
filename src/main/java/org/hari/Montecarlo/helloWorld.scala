package org.hari.Montecarlo

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

//test

object helloWorld {

  def main(args: Array[String]): Unit = {


    val sc = new SparkConf().setAppName("MonteCarlo").setMaster("local[*]")
    val sparkCont = new SparkContext(sc)
    val sqlCont = new HiveContext(sparkCont)

    val myList=Seq(1, 2, 3)


    val data = sparkCont.parallelize(myList)

    val data2= data

    data2.foreach(x=>println(x))


    println(data.count())

    import sqlCont.implicits._


    val df=data.toDF("col1")

    df.show()

  }

}
