package com.jarry.bigdata.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/18.
  */
object DataSourceApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);
        val sqlContext = new SQLContext(sc);

        //default format is parquet
        val df = sqlContext.read.load("resource/users.parquet");
        df.show();
        //df.select("name", "favorite_color").write.save("resource/name_favorite_color.parquet");
        df.select("name", "favorite_color").show();

        val df2 = sqlContext.read.format("json").load("resource/people.json");
        df2.select("name", "age").show();
    }
}
