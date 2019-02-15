package com.module.flink.example.worldcount.batch.n_001_source_file.n_001_worldCount_读取文件.n_001_读取文件内容

import org.apache.flink.api.scala.ExecutionEnvironment

object Run {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("src/main/resources/data/a.txt")


    val result = text.collect()

    println(s"打印结果:${result.mkString}" )


  }




}
