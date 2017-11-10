package com.zj.graphx

import java.io.{File, PrintWriter}

import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LPA {
  def main(args: Array[String]): Unit = {
    case class VD(){}
    case class ED(){}
    //    val conf = new SparkConf().setAppName("LPA").setMaster("local[3]")
//    val conf = new SparkConf().setAppName("LPA").setMaster("spark://192.168.52.165:7077")
    val conf = new SparkConf().setAppName("LPA").setMaster("spark://127.0.0.1:7077")
    val sparkContext = SparkContext.getOrCreate(conf)
    //    val edges: RDD[Edge[ED]] = sparkContext.textFile(LPA.getClass.getResource("/twitter/twitter.csv").getPath).map(line => {

    val edges: RDD[Edge[ED]] = sparkContext.textFile("twitter.csv").map(line => {
      val tokens = line.split(" ")
      Edge[ED](tokens(0).toLong, tokens(1).toLong, null)
    })
    val graph: Graph[VD, ED] = Graph.fromEdges[VD, ED](edges,null)
    val vWriter = new PrintWriter(new File("LAP.csv"))
    val lpaGraph:Graph[VertexId, ED]=LabelPropagation.run(graph,1000)
    lpaGraph.vertices.foreach(println)
    vWriter.close()
  }
}
