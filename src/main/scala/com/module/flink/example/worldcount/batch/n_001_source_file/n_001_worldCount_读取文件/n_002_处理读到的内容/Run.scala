package com.module.flink.example.worldcount.batch.n_001_source_file.n_001_worldCount_读取文件.n_002_处理读到的内容

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object Run {

  def main(args: Array[String]): Unit = {

    //val port = 1234
    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // get input data by connecting to the socket
    //val text = env.socketTextStream("localhost", port, '\n')
    val text = env.readTextFile("/opt/n_001_workspaces/bigdata/flink/flink-maven-scala/src/main/resources/data/a.txt")


    import org.apache.flink.streaming.api.scala._
    val textResult = text.flatMap( w => w.split("\\s") ).map( w => WordWithCount(w,1))
     // .keyBy("word")
      /**
        * 每5秒刷新一次，相当于重新开始计数，
        * 好处，不需要一直拿所有的数据统计
        * 只需要在指定时间间隔内的增量数据，减少了数据规模
        */
      //.timeWindow(Time.seconds(5))
     // .sum("count" )

    textResult.print().setParallelism(1)

    env.execute("文件单词统计作业")
    println("结束")

  }


  // Data type for words with count
  case class WordWithCount(word: String, count: Long){
    override def toString: String = word + ":" + count
  }

}
