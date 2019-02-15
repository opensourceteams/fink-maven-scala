package com.module.flink.example.worldcount.stream.n_001_source_nc.n_001_打印输入数据

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


object Run {

  def main(args: Array[String]): Unit = {

    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')

    text.print().setParallelism(1)

    env.execute("打印输入数据")

  }

}
