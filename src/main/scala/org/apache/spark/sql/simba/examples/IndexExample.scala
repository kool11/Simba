package org.apache.spark.sql.simba.examples

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
      //.master("local[4]")
      .appName("IndexExample")
      .config("simba.index.partitions", "64")
      .getOrCreate()

    //buildIndex(simbaSession)
    useIndex1(simbaSession)
    //useIndex2(simbaSession)
    simbaSession.stop()
  }

  private def buildIndex(simba: SimbaSession): Unit = {
    import simba.implicits._
    val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"), PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
      PointData(2.0, 2.0, 3.0, "4"), PointData(3.0, 3.0, 3.0, "5"), PointData(4.0, 4.0, 3.0, "6")).toDS

    datapoints.createOrReplaceTempView("a")
    //simba.indexTable(tableName = "a",HashMapType,"test",Array("x","y"))

    simba.indexTable("a", RTreeType, "testqtree", Array("x", "y"))

    simba.showIndex("a")
  }

  private def useIndex1(simba: SimbaSession): Unit = {
    import simba.implicits._
    import simba.simbaImplicits._
    //val datapoints = Seq(PointData(1.0, 1.0, 3.0, "1"),  PointData(2.0, 2.0, 3.0, "2"), PointData(2.0, 2.0, 3.0, "3"),
    //  PointData(2.0, 2.0, 3.0, "4"),PointData(3.0, 3.0, 3.0, "5"),PointData(4.0, 4.0, 3.0, "6")).toDF()

    val datapoint = simba.sparkContext.textFile("file:///home/ruanke/normal.csv").map(f => {
      val line = f.split(",").toList
      PointData(line(1).toDouble, line(2).toDouble, line(3).toDouble, line(0))
    }).toDS()
    datapoint.createOrReplaceTempView("b")

    var start = System.currentTimeMillis()
    simba.indexTable("b", RTreeType, "QuadTreeForData", Array("x", "y"))
    var end = System.currentTimeMillis()
    println("Create Index cost: "+(end-start))
    simba.showIndex("b")


    var offset = scala.util.Random.nextInt()
    var a = 0
    for(a <- 1 to 10){
      var x = scala.util.Random.nextInt(5)+scala.util.Random.nextFloat()+offset
      var y = scala.util.Random.nextInt(5)+scala.util.Random.nextFloat()+offset
      println("x:"+x+" y:"+y)
      start = System.currentTimeMillis()
      val res = simba.sql("SELECT * FROM b")
      res.knn(Array("x", "y"), Array(x, y), 10).show(10)
      end = System.currentTimeMillis()
      println("query cost: "+(end-start))
    }


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
