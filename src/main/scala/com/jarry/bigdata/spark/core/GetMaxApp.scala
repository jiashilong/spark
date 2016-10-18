package com.jarry.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/17.
  */
object GetMaxApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);
        val path = args(0)

        val lines = sc.textFile(path)
        val max = lines.flatMap(line => line.split("\\s")).reduce((a,b) => if(a>b) a else b);
        println("max is " + max)
    }
}
