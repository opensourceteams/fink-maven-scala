package com.module.flink.example.worldcount.stream.n_004_window.n_002_timeWindow

import java.io.File

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time




object Run {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.readTextFile("/opt/n_001_workspaces/bigdata/flink/flink-maven-scala/src/main/resources/data/a.txt")


    deleteDir(new File("/opt/n_001_workspaces/bigdata/flink/flink-maven-scala/src/main/resources/dataoutput/b"))

    import org.apache.flink.streaming.api.scala._
    val textResult = text.flatMap( w => w.split("\\s") ).map( w => (w,1))
      .keyBy(0)
      /**
        * 每5秒刷新一次，相当于重新开始计数，
        * 好处，不需要一直拿所有的数据统计
        * 只需要在指定时间间隔内的增量数据，减少了数据规模
        */
      .timeWindow(Time.milliseconds(1))

      /**
        * size:元素数量
        * slide:元素数量的间隔
        */
      //.countWindow(2,1)
      .sum(1 )

    textResult.writeAsText("/opt/n_001_workspaces/bigdata/flink/flink-maven-scala/src/main/resources/dataoutput/b")
    textResult.print().setParallelism(1)

    env.execute("文件单词统计作业")
    println("结束")

  }



  /**
    * 删除一个文件夹,及其子目录
    * @param dir
    */
  def deleteDir(dir: File): Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if (f.isDirectory) {
        deleteDir(f)
      } else {
        f.delete()
        println("delete file " + f.getAbsolutePath)
      }
    })
    dir.delete()
    println("delete dir " + dir.getAbsolutePath)
  }


}
