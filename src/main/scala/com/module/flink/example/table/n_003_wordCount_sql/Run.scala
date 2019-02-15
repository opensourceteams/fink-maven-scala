package com.module.flink.example.table.n_003_wordCount_sql

import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._

object Run {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tEnv = TableEnvironment.getTableEnvironment(env)



    val orderA: DataStream[WordWithCount] = env.fromCollection(Seq(
      WordWithCount( "beer", 1),
      WordWithCount( "diaper", 1),
      WordWithCount( "diaper", 1),
      WordWithCount( "diaper", 1),
      WordWithCount( "beer", 1),
      WordWithCount( "rubber", 1))
    )


    tEnv.registerDataStream("WordWithCount",orderA)


    val result = tEnv.sqlQuery(
      s"SELECT word,sum(count1) FROM WordWithCount group by  word ")

    result.toRetractStream[WordWithCount].writeAsText("src/main/resources/dataoutput/text/a",WriteMode.OVERWRITE)

    env.execute()






  }


  case class WordWithCount( word: String, count1: Int){
    override def toString: String = word +" , " + count1
  }




}
