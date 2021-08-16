package com.wsb.project

import org.apache.spark.sql.SparkSession

object TempObject {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TempObject")
      .getOrCreate()

    spark.sql("SELECT 1").show();
  }
}
