package com.module.flink.example.dataset.n_001_source_file.n_001_csv.n_003_csv_输出数据到csv文件

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableEnvironment, Types}
import org.apache.flink.table.sources.CsvTableSource

object CSVVoRun {

  def main(args: Array[String]): Unit = {



    val env = ExecutionEnvironment.getExecutionEnvironment
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


    table.toDataSet[Student].writeAsCsv("src/main/resources/dataoutput/csv/user","\n",",",WriteMode.OVERWRITE)

    env.execute()




  }


  case class Student(name: String, age: Int)

}
