package com.jarry.bigdata.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext


/**
  * Created by jarry on 16/10/17.
  */
object DataFrameApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);
        val sqlContext = new SQLContext(sc);

        val df = sqlContext.read.json(args(0));
        df.show();
        df.printSchema();
        df.select("name").show();
        df.select(df("name"), df("age")+1).show();
        df.filter(df("age") > 21).show();
        df.groupBy("age").count().show();
    }


}
