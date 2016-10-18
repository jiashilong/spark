package com.jarry.bigdata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/17.
  */
object WordCountApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);
        val path = args(0);

        val lines = sc.textFile(path);
        val cs = lines.flatMap(line => line.split("\\s")).map(line => (line, 1)).reduceByKey((a, b) => a+b);
        cs.foreach(line => println(line));
    }
}
