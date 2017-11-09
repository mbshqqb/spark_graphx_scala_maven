package com.zj

import org.apache.spark._
import org.apache.spark.graphx._

object Main {
  case class IdentityVertex(uid:Long, arm_uid:Long, level:Int, var isFlag:Boolean, var score:Double){}
  case class IdentityEdge(){}
  case class Message(isReserved:Boolean){}
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("pagerank").setMaster("local[3]")
    val sparkContext= SparkContext.getOrCreate(conf)
    val identics=sparkContext.textFile(Main.getClass.getResource("/ky_uid_min3000.csv").getPath,100).flatMap(line=>{
      try{
        val tokens= line.split(",")
        val uid =tokens(0).replace("_","").toLong
        val arm_uid =tokens(1).replace("_", "").toLong
        val level =tokens(2).toInt
        Option(IdentityVertex(uid,arm_uid,level,false,0))
      }catch {
        case ex=> None
      }
    }).cache()
    val vertex=identics.map(iv=>(iv.uid,iv))
    val edge=identics.map(iv=>Edge[IdentityEdge](iv.uid,iv.arm_uid,null))
    val graph=Graph(vertex,edge).cache()
    val threshold = 10
    val levalMax = 200
    //pagerank
    val prVertices=graph.pageRank(0.01, 0.15).vertices
    //result
    graph.subgraph(edgeTriplet=>true,(v, d) =>d!=null)
      .joinVertices(prVertices)((vertexId, vd, u) =>IdentityVertex(vd.uid,vd.arm_uid,vd.level,u>threshold,u))
      .pregel(Message(false))((vertexId, vd, a) =>{ if (a.isReserved || vd.isFlag) vd.isFlag=true
        else vd.isFlag
        vd
      }, (edgeTriplet) => {
          if(edgeTriplet.srcAttr.isFlag)Some((edgeTriplet.dstId,Message(true))).iterator
          else None.iterator
      },(a,b) =>Message(a.isReserved||b.isReserved))
      .subgraph(edgeTriplet=>true,(vertexId, vd)=>vd.isFlag).vertices.saveAsTextFile("result")
  }
}