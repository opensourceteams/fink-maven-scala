package com.module.flink.example.worldcount.stream.n_003_source_Seq

import com.module.flink.util.file.CustomerFileUtil
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Run {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val seq = Seq[String]("数据A 数据B 数据C","数据A 数据B 数据C","数据A 数据B 数据C")
    import org.apache.flink.streaming.api.scala._
    val text = env.fromCollection(seq)



    val textResult = text.flatMap( w => w.split("\\s") ).map( w => (w,1))
      .keyBy(0)
      /**
        * 每5秒刷新一次，相当于重新开始计数，
        * 好处，不需要一直拿所有的数据统计
        * 只需要在指定时间间隔内的增量数据，减少了数据规模
        */
     // .timeWindow(Time.milliseconds(1))
      .sum(1 )

    val dirOutput = "/opt/n_001_workspaces/bigdata/flink/flink-maven-scala/src/main/resources/dataoutput/a"

    CustomerFileUtil.deleteDir(dirOutput)

    textResult.writeAsText(dirOutput)
  //  textResult.print().setParallelism(1)

    env.execute("文件单词统计作业")
    println("结束")

  }




}
