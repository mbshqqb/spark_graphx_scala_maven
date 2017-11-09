package com.trs

import java.io.{File, PrintWriter}

import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object PageRank {
  case class IdentityVertex(uid:Long, arm_uid:Long, level:Int, var isFlag:Boolean, var score:Double){}
  case class IdentityEdge(){}
  case class Message(isReserved:Boolean){}
  def main(args: Array[String]): Unit = {
    val partitionNumber=0
    val pagerankThreshold = 10
    val levalMax = 100
    val itera=100
//    val conf = new SparkConf().setAppName("pagerank").setMaster("local[3]")
//      val conf = new SparkConf().setAppName("pagerank").setMaster("spark://192.168.52.165:7077")
    val conf = new SparkConf().setAppName("pagerank").setMaster("spark://127.0.0.1:7077")

    val sparkContext= SparkContext.getOrCreate(conf)
//    val identics:RDD[IdentityVertex]=sparkContext.textFile(Main.getClass.getResource("/ky_uid_min3000.csv").getPath,partitionNumber).flatMap(line=>{
    val identics=sparkContext.textFile("./ky_uid.csv").flatMap(line=>{
    try{
        val tokens= line.split(",")
        Option(IdentityVertex(tokens(0).replace("_","").toLong,tokens(1).replace("_", "").toLong,tokens(2).toInt,false,0))
      }catch {
        case ex=>{
          ex.printStackTrace()
          None
        }
      }
    })
    val vertex:RDD[(VertexId, IdentityVertex)]=identics.map(iv=>(iv.uid,iv))
    val edge:RDD[Edge[IdentityEdge]]=identics.map(iv=>Edge[IdentityEdge](iv.uid,iv.arm_uid,null))
    val graph:Graph[IdentityVertex, IdentityEdge]=Graph(vertex,edge)

    val prVertices:VertexRDD[Double]=graph.pageRank(0.0001, 0.15).vertices
    prVertices.foreach(println(_))
    val resultVertics:RDD[IdentityVertex]=graph.subgraph(edgeTriplet=>true, (v, d) =>d!=null)
      .joinVertices(prVertices)((vertexId, vd, u) =>IdentityVertex(vd.uid,vd.arm_uid,vd.level,u>pagerankThreshold,u))
      .pregel(Message(false),itera,EdgeDirection.Out)((vertexId, vd, a) =>{
        if (a.isReserved || vd.isFlag) vd.isFlag=true
        else vd.isFlag
        vd
      }, (edgeTriplet) => {
          if(edgeTriplet.srcAttr.isFlag)Some((edgeTriplet.dstId,Message(true))).iterator
          else None.iterator
      },(a,b) =>Message(a.isReserved||b.isReserved))
      .subgraph(edgeTriplet=>true,(vertexId, vd)=>vd.isFlag)
      .vertices
      .map(_._2)
    val writer = new PrintWriter(new File("result.csv"))
    for (vertics <- resultVertics.collect())
      writer.println(vertics.uid,vertics.arm_uid,vertics.level,vertics.score)
    writer.close()
  }
}