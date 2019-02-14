package com.module.flink.example.worldcount.n_004_相同单词统计

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


object Run {

  def main(args: Array[String]): Unit = {

    val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    val text = env.socketTextStream("localhost", port, '\n')


    import org.apache.flink.streaming.api.scala._
    val textResult = text.flatMap( w => w.split("\\s") ).map( w => WordWithCount(w,1))
      .keyBy("word").reduce((a,b) => WordWithCount(a.word ,a.count + b.count )  )

    textResult.print().setParallelism(1)

    env.execute("打印输入数据")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long)

}
