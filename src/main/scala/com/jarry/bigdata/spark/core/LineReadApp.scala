package com.jarry.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/17.
  */
object LineReadApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf();
    val path = args(0);
    val sc = new SparkContext(conf);

    val lines = sc.textFile(path).collect();
    for(line <- lines) {
      println(line)
    }
  }
}
