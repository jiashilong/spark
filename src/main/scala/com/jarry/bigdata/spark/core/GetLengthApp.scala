package com.jarry.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/17.
  */
object GetLengthApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);

        val lines = sc.textFile(args(0));
        val len = lines.map(line => line.length()).reduce((a,b) => a+b);
        println("length is " + len)
    }
}
