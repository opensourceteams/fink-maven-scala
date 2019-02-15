package com.module.flink.example.worldcount.batch.n_001_source_file.n_001_worldCount_读取文件.n_001_读取文件内容

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object Run {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("/opt/n_001_workspaces/bigdata/flink/flink-maven-scala/src/main/resources/data/a.txt")



    text.print().setParallelism(1)

    env.execute("文件单词统计作业")
    println("结束")

  }




}
