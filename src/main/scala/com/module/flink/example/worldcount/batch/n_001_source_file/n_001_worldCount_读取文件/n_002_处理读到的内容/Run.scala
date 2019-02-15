package com.module.flink.example.worldcount.batch.n_001_source_file.n_001_worldCount_读取文件.n_002_处理读到的内容

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object Run {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("src/main/resources/data/a.txt")




    //val result = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }

    val result = text.flatMap( _.toLowerCase.split("\\s") filter(_.nonEmpty))
        .map((_,1))
        .groupBy(0)
        .sum(1)


    println(s"打印结果:${result.collect()}" )


  }




}
