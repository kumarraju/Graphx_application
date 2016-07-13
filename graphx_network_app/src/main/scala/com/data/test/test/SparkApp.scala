package com.data.test.test

import org.apache.spark._

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import scala.collection._

class SparkApp(args: Array[String]) {

  def StartProcessing(): Unit = {
    val file = args(0)
    val depth = args(1).toInt
    val conf = new SparkConf().setAppName("SoundCloud")
      .setMaster("local")

    val sc = new SparkContext(conf)
    val friends = sc.textFile(file).cache
    val friendlist = friends.flatMap { line =>
      line.split('\t')
    }
    val friend = friendlist.distinct.sortBy(x => x, true, 1).cache
    val friendWithIndex = friend.zipWithIndex

    val HashMapBrod = sc.broadcast(friendWithIndex.collectAsMap)
    
    val vertices = friendWithIndex.map(pair => pair.swap)

    val edges = friends.map { line =>
      val fields = line.split('\t')
      val p1 = HashMapBrod.value.get(fields(0)).head
      val p2 = HashMapBrod.value.get(fields(1)).head
      List(Edge(p1, p2, 0), Edge(p2, p1, 0))
    }.flatMap { r => r }.sortBy(_.attr, ascending = true)
    val Hashlookup = HashMapBrod.value.map(elem => (elem._2, elem._1))
    val graph = Graph(vertices, edges).cache()
    /* println("numVertices: " + graph.numVertices)
    println("numEdges: " + graph.numEdges)*/
    var finalRes = ""

    val Iterator = graph.collectNeighbors(EdgeDirection.Either).sortBy(_._1, true, 1).collect()
    for (iterate <- Iterator) {
      finalRes += Hashlookup.getOrElse(iterate._1, "none") + "\t"
      finalRes += userName(Hashlookup, processGraph(graph, iterate._2.distinct.map(elem => elem._1).toSet, iterate._1.toLong, depth)).mkString("\t") + "\n"
    }

    println(finalRes)
    sc.stop()
  

    def processGraph(graph: Graph[String, Int], inputSet: Set[Long], excludeVid: Long, depth: Int): Set[Long] = {
      var outputSet: Set[Long] = inputSet
      var temp_depth = depth
      println("temp_depth: " + temp_depth)
      if (temp_depth >= 2) {
        println("temp_depth: " + temp_depth)
        val Iterator1 = graph.collectNeighbors(EdgeDirection.Either).filter { case (vId, attlist1) => outputSet.contains(vId) }.collect()
        for (iterate <- Iterator1) {
          for (edge <- iterate._2) {
            outputSet = outputSet + edge._1
            //println("outputSet: " + outputSet)
          }
        }
        temp_depth -= 1
      }
      outputSet = outputSet - excludeVid
      // println("Final outputSet: " + outputSet)
      outputSet
    }

    def userName(hashlookup: Map[Long, String], vertexId: Set[Long]): List[String] = {
      var res: Set[String] = collection.mutable.Set()
      for (x <- vertexId.toIterator) {
        res += hashlookup.getOrElse(x, "none")
        /* println("vertexId: " + x)
        println("res: " + res.mkString(","))*/
      }
      res.toList.sorted
    }

  }
}