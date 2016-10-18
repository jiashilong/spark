package com.jarry.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/17.
  */
object LineFilterApp {
  def main(args: Array[String]): Unit = {
      if(args.length != 1) {
          throw new IllegalArgumentException("illegal argument");
      }
      val path = args(0);
      val conf = new SparkConf();
      val sc= new SparkContext(conf);

      val lines = sc.textFile(path).cache();
      val data1 = lines.filter(line => line.contains("1")).count();
      val data2 = lines.filter(line => line.contains("2")).count();
      println("lines with 1:%s,  2:%s".format(data1, data2));
  }
}
