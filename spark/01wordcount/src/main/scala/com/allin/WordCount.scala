package com.allin

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    // spark config
    val config: SparkConf =
      new SparkConf().setMaster("local").setAppName("wordcount")

    // spark context
    val sc = new SparkContext(config)

    val wcRdd = sc
      .textFile(
        WordCount.getClass.getClassLoader.getResource("words.txt").getPath
      )
      .flatMap(_.split(" "))
      .map((_, 1))
      .reduceByKey((_ + _))

    print(wcRdd.collect().toBuffer)
  }
}
