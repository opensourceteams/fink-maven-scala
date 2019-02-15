package com.module.flink.example.table.n_002_streamtable


import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.scala._

object Run {

  def main(args: Array[String]): Unit = {

    // get the execution environment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val tEnv = TableEnvironment.getTableEnvironment(env)



    val orderA: DataStream[Order] = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2))
    )



    val orderB: DataStream[Order] = env.fromCollection(Seq(
      Order(1L, "beer", 3),
      Order(1L, "diaper", 4),
      Order(3L, "rubber", 2))
    )


    // convert DataStream to Table



    val orderAll = orderA.union(orderB)


    val tableAll = tEnv.fromDataStream(orderAll, 'user, 'product, 'amount)

    val result = tEnv.sqlQuery(
      s"SELECT * FROM $tableAll WHERE amount > 2 ")

    result.toAppendStream[Order].writeAsText("src/main/resources/dataoutput/text/a",WriteMode.OVERWRITE)

    env.execute()






  }


  case class Order(user: Long, product: String, amount: Int)




}
