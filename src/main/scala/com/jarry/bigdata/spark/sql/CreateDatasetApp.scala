package com.jarry.bigdata.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Created by jarry on 16/10/17.
  */
case class People(name:String, age:Long);

object CreateDatasetApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);
        val sqlContext = new SQLContext(sc);
        import sqlContext.implicits._

        val ds = Seq(1, 2, 3).toDS();
        ds.map(d => d+1).collect().foreach(println);

        val dp = Seq(People("Andy", 32)).toDS();
        dp.collect().foreach(println);


        val people = sqlContext.read.json(args(0)).as[People];
        people.collect().foreach(println);
    }
}
