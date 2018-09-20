package com.sodad.els.triage.utils

case class PercentileElement (value: Double, ptile: Double)

class PTileNormalizer (repartition: Seq[PercentileElement]) extends (Double => Double) {
  def apply (x: Double) = {
    var p : Double = 0.0
    var i = 0
    while (i < repartition.size && x > repartition(i).value) {
      p = repartition(i).ptile
      i = i + 1
    }
    if (i == repartition.size) p = 1.0
    p
  }
}

object Kernels {
  type K[T, U] = (Seq[T], Seq[T]) => U
  def d2 (x: Seq[Double], y: Seq[Double]) : Double = 
    (x zip y).
      map { case (x, y) => x - y }.
      foldLeft (0.0) { (a, x) => a + x*x }
}
