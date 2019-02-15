package com.module.flink.example.worldcount.table.datastream.n_002_sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.TableEnvironment
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



    // convert DataStream to Table
    var tableA = tEnv.fromDataStream(orderA, 'user, 'product, 'amount)


    val result = tEnv.sqlQuery(
      s"SELECT * FROM $tableA WHERE amount > 2 ")

    result.toAppendStream[Order].print()

    env.execute()






  }


  case class Order(user: Long, product: String, amount: Int)




}
