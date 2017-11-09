package com.zj.graphx

import java.io.{File, PrintWriter}

import org.apache.spark.graphx.{Edge, Graph, GraphLoader, GraphXUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SubGraph_pr {
  case class Message(){}
  val pagerankThreshold = 20
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pagerank").setMaster("local[3]")
    val sparkContext= SparkContext.getOrCreate(conf)
    val edges:RDD[Edge[Double]]=sparkContext.textFile(SubGraph_pr.getClass.getResource("/tweeter/twitter.csv").getPath).map(line=>{
      val tokens=line.split(" ")
      Edge[Double](tokens(0).toLong,tokens(1).toLong,0)
    })

    val graph:Graph[Double, Double]=Graph.fromEdges[Double,Double](edges,0)
    val prVertices=graph.pageRank(0.0001,0.15).vertices
    val vWriter = new PrintWriter(new File("vertices.csv"))
    val eWriter = new PrintWriter(new File("edges.csv"))
    val resultGraph=graph.joinVertices(prVertices)((vertexId, vd, u) =>u).subgraph(x => true,(v, d) =>d>pagerankThreshold)
    resultGraph.vertices.collect().foreach(vWriter.println)
    resultGraph.edges.map(e=>(e.srcId,e.dstId)).collect().foreach(eWriter.println)
    vWriter.close()
    eWriter.close()
  }
}