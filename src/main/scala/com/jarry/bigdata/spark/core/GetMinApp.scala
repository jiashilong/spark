package com.jarry.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}
import java.lang.Math

/**
  * Created by jarry on 16/10/17.
  */
object GetMinApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);

        val lines = sc.textFile(args(0));
        val min = lines.flatMap(line => line.split("\\s")).reduce((a, b) => if(a<b) a else b);
        print("min is " + min)
    }
}
