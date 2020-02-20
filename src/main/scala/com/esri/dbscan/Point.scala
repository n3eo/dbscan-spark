package com.esri.dbscan

import scala.collection.mutable.ArrayBuffer

/**
  * Location in 2D space.
  *
  * @param id the point identifier.
  * @param x  the horizontal 2D placement.
  * @param y  the vertical 2D placement.
  */
class Point(val id: Long, val x: Double, val y: Double, val z: Double) extends Euclid {

  /**
    * Convert this point to a sequence of <code>Cell</code> instances based on its location in its parent cell.
    * The parent cell is the cell whose envelope wholly contains this point.
    * If the point is within <code>eps</code> distance of the edge of the cell, the neighboring cell is added to the aforementioned sequence.
    *
    * @param cellSize the parent cell size
    * @param eps      the neighborhood distance.
    * @return the parent cell and all the neighboring cells if the point is close to the edge.
    */
  def toCells(cellSize: Double, eps: Double): Seq[Cell] = {
    val xfac = (x / cellSize).floor
    val yfac = (y / cellSize).floor
    val zfac = (z / cellSize).floor

    val cx = xfac * cellSize
    val cy = yfac * cellSize
    val cz = zfac * cellSize

    val xmin = cx + eps
    val ymin = cy + eps
    val zmin = cz + eps

    val xmax = cx + cellSize - eps
    val ymax = cy + cellSize - eps
    val zmax = cz + cellSize - eps

    val row = yfac.toInt
    val col = xfac.toInt
    val lay = zfac.toInt

    val cellArr = new ArrayBuffer[Cell](4)
    cellArr += Cell(row, col, lay)

    if (z < zmin) {
      if (x < xmin) {
        cellArr += Cell(row, col - 1, lay -1)
        if (y < ymin) {
          cellArr += Cell(row - 1, col - 1, lay -1)
          cellArr += Cell(row - 1, col, lay -1)
        } else if (y > ymax) {
          cellArr += Cell(row + 1, col - 1, lay -1)
          cellArr += Cell(row + 1, col, lay -1)
        }
      } else if (x > xmax) {
        cellArr += Cell(row, col + 1, lay -1)
        if (y < ymin) {
          cellArr += Cell(row - 1, col + 1, lay -1)
          cellArr += Cell(row - 1, col, lay -1)
        } else if (y > ymax) {
          cellArr += Cell(row + 1, col + 1, lay -1)
          cellArr += Cell(row + 1, col, lay -1)
        }
      } else if (y < ymin) {
        cellArr += Cell(row - 1, col, lay -1)
      } else if (y > ymax) {
        cellArr += Cell(row + 1, col, lay -1)
      }
    } else if ( z > zmax) {
      if (x < xmin) {
        cellArr += Cell(row, col - 1, lay+1)
        if (y < ymin) {
          cellArr += Cell(row - 1, col - 1, lay+1)
          cellArr += Cell(row - 1, col, lay+1)
        } else if (y > ymax) {
          cellArr += Cell(row + 1, col - 1, lay+1)
          cellArr += Cell(row + 1, col, lay+1)
        }
      } else if (x > xmax) {
        cellArr += Cell(row, col + 1, lay+1)
        if (y < ymin) {
          cellArr += Cell(row - 1, col + 1, lay+1)
          cellArr += Cell(row - 1, col, lay+1)
        } else if (y > ymax) {
          cellArr += Cell(row + 1, col + 1, lay+1)
          cellArr += Cell(row + 1, col, lay+1)
        }
      } else if (y < ymin) {
        cellArr += Cell(row - 1, col, lay+1)
      } else if (y > ymax) {
        cellArr += Cell(row + 1, col, lay+1)
      }
    } else {
      if (x < xmin) {
        cellArr += Cell(row, col - 1, lay)
        if (y < ymin) {
          cellArr += Cell(row - 1, col - 1, lay)
          cellArr += Cell(row - 1, col, lay)
        } else if (y > ymax) {
          cellArr += Cell(row + 1, col - 1, lay)
          cellArr += Cell(row + 1, col, lay)
        }
      } else if (x > xmax) {
        cellArr += Cell(row, col + 1, lay)
        if (y < ymin) {
          cellArr += Cell(row - 1, col + 1, lay)
          cellArr += Cell(row - 1, col, lay)
        } else if (y > ymax) {
          cellArr += Cell(row + 1, col + 1, lay)
          cellArr += Cell(row + 1, col, lay)
        }
      } else if (y < ymin) {
        cellArr += Cell(row - 1, col, lay)
      } else if (y > ymax) {
        cellArr += Cell(row + 1, col, lay)
      }
    }
    cellArr
  }

  /**
    * @return text representation of this instance.
    */
  override def toString(): String = s"Point($id,$x,$y,$z)"

}

/**
  * Companion object to build a point
  */
object Point extends Serializable {
  /**
    * Instantiate a point given an id, x and y value.
    *
    * @param id the point identifier
    * @param x  the horizontal placement
    * @param y  the vertical placement
    * @return a new <code>Point</code> instance.
    */
  def apply(id: Long, x: Double, y: Double, z: Double) = {
    new Point(id, x, y, z)
  }
}
