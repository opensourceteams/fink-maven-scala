package com.module.flink.example.datastream.n_001_source_file.n_001_csv.n_003_csv_输出数据到csv文件

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sources.CsvTableSource

object StreamCSVVoRun {

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

    ds.writeAsCsv("src/main/resources/dataoutput/csv/user")
    env.execute()




  }


  case class Student(name: String, age: Int)

}
