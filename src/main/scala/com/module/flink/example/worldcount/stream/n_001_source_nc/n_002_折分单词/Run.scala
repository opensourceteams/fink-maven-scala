package com.module.flink.example.worldcount.stream.n_001_source_nc.n_002_折分单词

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object Run {

  def main(args: Array[String]): Unit = {

    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')


    import org.apache.flink.streaming.api.scala._
    // \s 匹配任何空白字符,包括空格、制表符、换页符等等
    val textFlatMap = text.flatMap { w => w.split("\\s") }

    textFlatMap.print().setParallelism(1)

    env.execute("打印输入数据")

  }

}
