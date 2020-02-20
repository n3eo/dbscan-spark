package com.esri.dbscan

/**
  * Cell in a fishnet represented by a row and a column value.
  */
case class Cell(val row: Int, val col: Int, val lay: Int) {

  /**
    * Get the envelope of the cell
    *
    * @param cellSize the cell size
    * @return the cell envelope
    */
  def toEnvp(cellSize: Double): Envp = {
    val xmin = col * cellSize
    val ymin = row * cellSize
    val zmin = lay * cellSize

    val xmax = xmin + cellSize
    val ymax = ymin + cellSize
    val zmax = zmin + cellSize

    Envp(xmin, ymin, zmin, xmax, ymax, zmax)
  }

}
