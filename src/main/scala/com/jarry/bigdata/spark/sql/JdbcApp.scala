package com.jarry.bigdata.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jarry on 16/10/18.
  */
object JdbcApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf();
        val sc = new SparkContext();
        val sqlContext = new SQLContext(sc);

        val url = "jdbc:mysql://localhost:3306/canal?user=canal&password=canal";
        val jdbcDF = sqlContext.read.format("jdbc").options(Map("url" -> url,
                                                                "dbtable" -> "canal.user")
                                                           ).load();
        jdbcDF.registerTempTable("t_user");
        val df = sqlContext.sql("select id, name, password from t_user where id>=103");
        df.show();
    }
}
