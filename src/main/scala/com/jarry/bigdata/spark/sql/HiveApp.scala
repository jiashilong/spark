package com.jarry.bigdata.spark.sql

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/18.
  */
object HiveApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("HiveApp").setMaster("local");
        val sc  = new SparkContext(conf);
        val hiveContext = new HiveContext(sc);
        println(hiveContext);
    }
}
