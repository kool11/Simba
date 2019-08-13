package org.apache.spark.sql.simba.examples

import java.io.{BufferedReader, FileWriter}

import org.apache.spark.sql
import org.apache.spark.sql.simba
import org.apache.spark.sql.simba.{Dataset, SimbaSession}
import org.apache.spark.sql.simba.index.{HashMapType, QuadTreeType, RTreeType, TreapType}

/**
  * Created by dongx on 3/7/2017.
  */
object IndexExample {

  case class PointData(x: Double, y: Double, z: Double, other: String)

  def main(args: Array[String]): Unit = {
    val simbaSession = SimbaSession
      .builder()
//      .master("local[4]")
      .appName("IndexExample")
      .config("simba.index.partitions", "200")
      .getOrCreate()
    if(args.length>=1){
      //buildIndex(simbaSession,args(0))
      //println(args(0))
      useIndex1(simbaSession,args)
    }
//    useIndex1(simbaSession,args)
    //useIndex2(simbaSession)
    simbaSession.stop()
  }

  private def buildIndex(simba: SimbaSession ,fileName:String): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 9.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"), PointData(3.0, 3.0, 3.0, "5"), PointData(4.0, 4.0, 3.0, "6")).toDS

//      val datapoints = simba.sparkContext.textFile("file:///home/ruanke/normal.csv").map(f => {
//        val line = f.split(",").toList
//        PointData(line(1).toDouble, line(2).toDouble, line(3).toDouble, line(0))
//      }).toDS()

    datapoints.createOrReplaceTempView("a")
    //simba.indexTable(tableName = "a",HashMapType,"test",Array("x","y"))

    simba.indexTable("a", RTreeType, "testqtree", Array("x", "y"))

    simba.showIndex("a")
    val fileName = "file:///d:\\Index"

    //simba.persistIndex("testqtree", "file://"+fileName)

  }

  private def useIndex(simba: SimbaSession): Unit ={
    import simba.implicits._
//    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
//      PointData(2.0, 2.0, 3.0, "4"), PointData(3.0, 3.0, 3.0, "5"), PointData(4.0, 4.0, 3.0, "6")).toDS
//    val sc = SimbaSession.getDefaultSession.get.sparkContext
    val datapoints = simba.sparkContext.textFile("file:///home/ruanke/normal.csv").map(f => {
        val line = f.split(",").toList
        PointData(line(1).toDouble, line(2).toDouble, line(3).toDouble, line(0))
      }).toDS()
   val fileName = "file:///d:\\Index"
    import simba.simbaImplicits._
    datapoints.loadIndex("testqtree", "file://"+fileName)
    //import simba.simbaImplicits._
    val res = datapoints.knn(Array("x", "y"), Array(10.0, 10), 5)
    println(res.queryExecution)
    res.show()
  }

  private def useIndex1(simba: SimbaSession, args:Array[String]): Unit = {
    import simba.implicits._
    //import simba.simbaImplicits._
    //val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
    //  PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()

    val datapoints = simba.sparkContext.textFile("file:///home/ruanke/normal.csv").map(f => {
      val line = f.split(",").toList
      PointData(line(1).toDouble, line(2).toDouble, line(3).toDouble, line(0))
    }).toDS()
    datapoints.createOrReplaceTempView("b")

    var start = System.currentTimeMillis()
    simba.indexTable("b", RTreeType, "QuadTreeForData", Array("x", "y"))
    var end = System.currentTimeMillis()
    println("Create Index cost: "+(end-start))
    val fileName = "file:///home/ruanke/work/"+args(0)
    import simba.simbaImplicits._
    //datapoints.loadIndex("testqtree", fileName)
    simba.showIndex("b")

    val res = simba.sql("SELECT * FROM b")

    var a = 0
    import scala.collection.mutable.Set
    var test:Set[(Double,Double)]= Set()
    var costTime:Set[Long] = Set()
    //var offset = scala.util.Random.nextInt(2000)
    var total = 0L
    import java.io.FileReader
    import java.io.BufferedReader
//    val in = new FileReader("/home/ruanke/work/simba/Simba/query_100.txt")
    val in = new FileReader("/home/ruanke/work/simba/Simba/"+args(1))
    val reader = new BufferedReader(in)
    var s = reader.readLine()
    while(s!=null){
      if(s.charAt(0)=='('&&s.charAt(s.length-1)==')'){
        val index = s.indexOf(',')
        val x1 = s.substring(1,index).toDouble
        val y1 = s.substring(index+1,s.length-1).toDouble
        test.add(x1,y1)
      }
      s= reader.readLine()
    }

    for(a <- test){
      val x:Double = a._1
      val y:Double = a._2
      //println("x:"+x+" y:"+y)
      start = System.currentTimeMillis()
      var i = 0
      while(i<5){
         res.range(Array("x", "y"), Array(x, y), Array(x,y)).collect()
         Thread.sleep(2000) 
         i = i+1
      }
      //res.range(Array("x", "y"), Array(x, y), Array(x*10,y*10)).collect()
      
      //res.knn(Array("x", "y"), Array(x, y), 1000000).collect()
      end = System.currentTimeMillis()
      val temp = end-start
      total = total+temp
      costTime.add(temp)
      Thread.sleep(5000)
      //println("query cost: "+(end-start))
    }
    import java.io.FileWriter
//    val out2 = new FileWriter("/home/ruanke/work/test/Simba/test.txt",true)
    val out2 = new FileWriter("/home/ruanke/work/test/Simba/"+args(2),true)
    for(i<-costTime) {
      out2.write(i.toString+"\n")
    }
    out2.close()

    println(total/test.size)
    //res.knn(Array("x", "y"),Array(2.0, 1.0),1).show(4)
  }

  private def useIndex2(simba: SimbaSession): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"), PointData(3.0, 3.0, 3.0, "5"), PointData(4.0, 4.0, 3.0, "6")).toDF()

    datapoints.createOrReplaceTempView("b")

    var start = System.currentTimeMillis()
    simba.indexTable("b", RTreeType, "RtreeForData", Array("x", "y"))
    var end = System.currentTimeMillis()
    println("Create Index cost: "+(end-start))
    simba.showIndex("b")

    start = System.currentTimeMillis()
    simba.sql("SELECT * FROM b where b.x >1 and b.y<=2").show(5)
    end = System.currentTimeMillis()
    println("query cost: "+(end-start))
  }

  private def useIndex3(simba: SimbaSession): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(0.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"), PointData(3.0, 3.0, 3.0, "5"), PointData(4.0, 4.0, 3.0, "6")).toDS()

    import simba.simbaImplicits._

    datapoints.index(TreapType, "indexForOneTable", Array("x"))

    datapoints.range(Array("x"), Array(1.0), Array(2.0)).show(4)
  }
}
