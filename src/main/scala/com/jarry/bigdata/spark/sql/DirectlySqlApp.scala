package com.jarry.bigdata.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/18.
  */
object DirectlySqlApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);
        val sqlContext = new SQLContext(sc);

        val df = sqlContext.sql("select * from parquet.`resource/users.parquet`");
        df.show();

        val df2 = sqlContext.sql("select * from json.`resource/people.json`");
        df2.show();
    }
}
