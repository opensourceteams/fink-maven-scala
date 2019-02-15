package com.module.flink.example.worldcount.batch.n_002_wirte_file.n_001_输出数据写到文件中

import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

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
    val outputFileDir = "src/main/resources/dataoutput/c"
    //CustomerFileUtil.deleteDir(outputFileDir)

    //重写
    result.writeAsText(outputFileDir,WriteMode.OVERWRITE)

    //不重写
    //result.writeAsText(outputFileDir,WriteMode.NO_OVERWRITE)


   // println(s"打印结果:${result.collect()}" )


  }




}
