package com.module.flink.example.worldcount.n_003_拆分单词为KeyValue

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object Run {

  def main(args: Array[String]): Unit = {

    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')


    import org.apache.flink.streaming.api.scala._
    val textResult = text.flatMap( w => w.split("\\s") ).map( w => (w,1))

    textResult.print().setParallelism(1)

    env.execute("打印输入数据")

  }

}
