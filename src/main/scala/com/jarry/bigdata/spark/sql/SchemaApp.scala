package com.jarry.bigdata.spark.sql

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/17.
  */
object SchemaApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext(conf);
        val sqlContext = new SQLContext(sc);

        val people = sc.textFile(args(0));
        val schemaString = "name age";
        val schema = StructType(schemaString.split("\\s")
                                            .map(fieldName => StructField(fieldName, StringType, true)));
        val row = people.map(_.split(",")).map(p => Row(p(0), p(1)));
        val df = sqlContext.createDataFrame(row, schema);
        df.registerTempTable("t_people");

        val teenagers = sqlContext.sql("SELECT name, age FROM t_people WHERE age >= 13 AND age <= 19");
        teenagers.show();

        teenagers.map(p => "Name:" + p(0) + ",Age:" + p(1) ).collect().foreach(println);
    }
}
