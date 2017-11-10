package com.zj.graphx

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, Graph, GraphLoader, VertexRDD}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object SecondaryDegreeRelationship {
  case class VD(map:mutable.Map[Long,Int]){}
  case class ED(){}
  case class Message(){}
  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("SecondaryDegreeRelationship").setMaster("local[3]")
    val conf = new SparkConf().setAppName("SecondaryDegreeRelationship").setMaster("spark://127.0.0.1:7077")
    val sparkContext= SparkContext.getOrCreate(conf)
    val edges:RDD[Edge[ED]]=sparkContext.textFile(SubGraph_pr.getClass.getResource("/twitter/test.csv").getPath).map(line=>{
      val tokens=line.split(",")
      Edge[ED](tokens(0).toLong,tokens(1).toLong,null)
    })
    val graph:Graph[VD, ED]=Graph.fromEdges[VD,ED](edges,VD(mutable.Map[Long,Int]()))
    //需要将图进行翻转
    val reversalGraph=graph.reverse
    val degreeRelation_1:VertexRDD[mutable.Map[Long, Int]]=reversalGraph.aggregateMessages[mutable.Map[Long,Int]](triplet=>{
      triplet.sendToDst(triplet.srcAttr.map.+((triplet.srcId,1)))
    },_++_)
    val degreeRelation_2:VertexRDD[mutable.Map[Long, Int]]=reversalGraph.outerJoinVertices(degreeRelation_1)((vertexId, oldVD, mapOption) =>mapOption.getOrElse(mutable.Map[Long,Int]())).aggregateMessages[mutable.Map[Long,Int]](triplet=>{
      val message=triplet.srcAttr.map(t=>(t._1,t._2+1)).+((triplet.srcId,1))
      triplet.sendToDst(message)
    },(m1:mutable.Map[Long,Int],m2:mutable.Map[Long,Int])=>{
      (m1/:m2){case(m,(k,v))=>m+(k->Math.min(v,m.getOrElse(k,v)))}
    })
    graph.outerJoinVertices(degreeRelation_2)((vertexId, oldVD, mapOption) =>mapOption.getOrElse(mutable.Map[Long,Int]())).vertices.foreach(println)
  }
}
