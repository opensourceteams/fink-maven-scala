package com.module.flink.util.file

import java.io.File

object CustomerFileUtil {

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


  /**
    * 删除一个文件夹,及其子目录
    * @param dir
    */
  def deleteDir(dir: String): Unit = {
    deleteDir(new File(dir))
  }



}
