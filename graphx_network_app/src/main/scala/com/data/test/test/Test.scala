package com.data.test.test

import scala.math.random
import org.apache.spark._
 
/**
 * @author Kumar
 */
object App {
  def main(args: Array[String]) {
    println("Starting Spark application .....")
    val SparkApp=new SparkApp(args)
    SparkApp.StartProcessing()
  }

}
