package com.jarry.bigdata.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/17.
  */

case class Person(name:String, age:Int);

object QueryApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);
        val sqlContext = new SQLContext(sc);
        import sqlContext.implicits._

        val people = sc.textFile(args(0))
                       .map(line => line.split(","))
                       .map(p => Person(p(0).trim(), p(1).trim.toInt))
                       .toDF();
        people.registerTempTable("t_people");
        val teenagers = sqlContext.sql("SELECT name, age FROM t_people WHERE age >= 13 AND age <= 19");
        teenagers.show();

        teenagers.map(p => "Name:" + p(0) + ",Age:" + p(1) ).collect().foreach(println);
        teenagers.map(p => "Name:" + p.getAs[String]("name") + ",Age:" + p.getAs[Int]("age"))
                 .collect().foreach(println);
        teenagers.map(_.getValuesMap(List("name", "age"))).collect().foreach(println);
    }
}
