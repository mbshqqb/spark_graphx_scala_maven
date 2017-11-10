package com.zj.graphx

import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

object CC {
  def main(args: Array[String]): Unit = {
    case class VD(){}
    case class ED(){}
        val conf = new SparkConf().setAppName("CC").setMaster("local[3]")
//    val conf = new SparkConf().setAppName("CC").setMaster("spark://192.168.52.165:7077")
//    val conf = new SparkConf().setAppName("CC").setMaster("spark://127.0.0.1:7077")

    val sparkContext = SparkContext.getOrCreate(conf)
    val edges: RDD[Edge[ED]] = sparkContext.textFile(CC.getClass.getResource("/twitter/twitter_min8000.csv").getPath).flatMap(line => {
      try{
        val tokens = line.split(" ")
        Option(Edge[ED](tokens(0).toLong, tokens(1).toLong, null))
      }catch {
        case ex=>{
          ex.printStackTrace()
          None
        }
      }
    })
    val graph: Graph[VD, ED] = Graph.fromEdges[VD, ED](edges,VD())
    val vWriter = new PrintWriter(new File("CC.csv"))
    graph.connectedComponents(1).vertices.collect.foreach(vWriter.println)
    vWriter.close()
  }
}
