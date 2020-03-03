package com.esri.dbscan

import java.io.{File, FileReader}
import java.util.Properties

import com.esri.dbscan.DBSCANStatus.DBSCANStatus
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.{log, ceil, pow}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{Dataset, Row, DataFrame}

import org.apache.spark

object DBSCANApp {

  type Cluster = (Int, Int, Int, Int) // layId, rowID, colID, clusterID

  var eps : Double = 3
  var minPoints : Int = 10
  var cellSize : Double = 0
  var numPartitions : Int = 0
  var numPointsPerPartition : Int = 2000

  private var fieldId = 0
  private var fieldX = 1
  private var fieldY = 2
  private var fieldZ = 3

  private def dbscan(sc: SparkContext,
             points: RDD[Point],
             eps: Double,
             minPoints: Int,
             cellSize: Double,
             numPartitions: Int
            ): RDD[DBSCANPoint] = {

    val emitted = points
      // Emit each point to all neighboring cell (if applicable)
      .flatMap(point => point.toCells(cellSize, eps).map(_ -> point))
      .groupByKey(numPartitions)
      .flatMap { case (cell, pointIter) => {
        val border = cell.toEnvp(cellSize)
        // Create inner envp
        val inside = border.shrink(eps)
        // Convert the points to dbscan points.
        val points = pointIter
          .map(point => {
            DBSCANPoint(point, cell.row, cell.col, cell.lay, border.isInside(point), inside.toEmitID(point))
          })
        // Perform local DBSCAN on all the points in that cell and identify each local cluster with a negative non-zero value.
        DBSCAN2(eps, minPoints).cluster(points)
      }
      }
      .cache()

    // Create a graph that relates the distributed local clusters based on their common emitted points.
    val graph = emitted
      .filter(_.emitID > 0)
      .map(point => point.id -> (point.row, point.col, point.lay, point.clusterID))
      .groupByKey(numPartitions)
      .aggregate(Graph[Cluster]())(
        (graph, tup) => {
          val orig = tup._2.head
          tup._2.tail.foldLeft(graph) {
            case (g, dest) => g.addTwoWay(orig, dest)
          }
        },
        _ + _
      )

    val globalBC = sc.broadcast(graph.assignGlobalID())

    // Relabel the 'non-noisy' points wholly inside the cell to their global id and write them to the specified output path.
    emitted
      .filter(_.inside)
      .mapPartitions(iter => {
        val globalMap = globalBC.value
        iter.map(point => {
          val key = (point.row, point.col, point.lay, point.clusterID)
          point.clusterID = globalMap.getOrElse(key, point.clusterID)
          point
        })
      })
  }

  def doMain(sc: SparkContext, df: DataFrame): DataFrame = {
    val points: RDD[Point] = df.select("index", "index1_percent", "index2_percent", "hausdorff_percent").rdd.flatMap( 
      row => {
         Some(Point(row.getLong(0), row.getDouble(1), row.getDouble(2), row.getDouble(2)))
      })

    val total = points.count

    if (this.numPointsPerPartition == 0) {
      this.numPointsPerPartition = 2000
    }
    if (this.numPartitions == 0) {
      this.numPartitions = pow(2, (log( (total / this.numPointsPerPartition) )/log(2)).ceil).toInt
    }
    if (this.eps == 0) {
      this.eps = 2
    }
    if (this.cellSize == 0) {
      this.cellSize = this.eps * 10
    }    
    if (this.minPoints == 0) {
      this.minPoints = (total * 0.2).ceil.toInt
    } else if ( total == 1){
      this.minPoints = 1
    // because of 20%
    } else if ( total < 10) {
      this.minPoints = 2
    }
    
    val clustered_points = dbscan(sc, points, this.eps, this.minPoints, this.cellSize, this.numPartitions)

    val sqlContext= new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    clustered_points.map( point => {
        (point.id, point.clusterID)
      }).toDF("index","cluster_id")
  }
}