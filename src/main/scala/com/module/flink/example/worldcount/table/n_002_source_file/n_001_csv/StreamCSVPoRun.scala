package com.module.flink.example.worldcount.table.n_002_source_file.n_001_csv

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

object StreamCSVPoRun {

  def main(args: Array[String]): Unit = {



    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)


    val customerSource = CsvTableSource.builder()
      .path("src/main/resources/data/csv/user.csv")
      .ignoreFirstLine()
      .fieldDelimiter(",")
      .field("name", Types.STRING)
      .field("age", Types.INT)
      .build()



    tEnv.registerTableSource("customers", customerSource)

    val table = tEnv
      .scan("customers")
      //.select('name, 'age)

    // convert it to a data stream
    val ds = table.toAppendStream[Student]

    ds.print()
    env.execute()




  }


  case class Student(name: String, age: Int)

}
