package com.sodad.els.triage.manuscripts
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.sodad.els.triage.config._
import com.sodad.els.triage.utils._

/*
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
 */

class TrendingApp (val embeddingApp : EmbeddingApp) {
  import embeddingApp._
  import session.implicits._

  def trendingForEmbedding (
    subEmbedding: Seq[Double], 
    targetIssn: String, 
    subYear: Int,
    normalizer: Double => Double
  ) : Option[Double] = {
    val kernel = (x: Seq[Double], y: Seq[Double]) =>
    (x zip y).
      map { case (x, y) => x - y }.
      foldLeft (0.0) { (a, x) => a + x*x }
    val journalCentroids = 
      pr (pc.getPathJournalsEmbeddingPerYear).
        filter ($"id" === targetIssn && $"year" < subYear).
        orderBy($"year".desc).
        as[EmbeddingRecordWithYear[Double]].
        collect

    val d2_diff = journalCentroids.map { x => kernel (x.embedding, subEmbedding) }
    
    if (d2_diff.size < 2) 
      None
    else 
      Some (normalizer (d2_diff (1) - d2_diff (0)))
  }

  def trendingForEid (
    subEid: String,
    targetIssn: String, 
    normalizer: Double => Double
  ) = {
    val subEmbedding = 
      manuscriptsEmbeddings.
        filter ($"id" === subEid).
        head.
        embedding
    val subYear: Int = manuscripts.
      filter ($"eid" === subEid).
      as[ManuscriptsContentRecord].
      head.
      year
    trendingForEmbedding (subEmbedding, targetIssn, subYear, normalizer)
  }

  def trendingForText (
    subText: String, 
    targetIssn: String, 
    subYear: Int, 
    normalizer: Double => Double
  ) = {
    val wordEmbeddingData =
      pr (pc.getPathWordEmbeddingOnAbstracts).
        toDF ("word", "embedding").
        as[WordEmbeddingRecord]
    val weightData = 
      pr (pc.getPathTokensIdfWeights).
        as[(String, Double)]
    val subEmbedding = computeTextEmbedding (
      Seq(subText),
      wordEmbeddingData,
      weightData)
    trendingForEmbedding (subEmbedding (0), targetIssn, subYear, normalizer)
  }

  def computeTrendingRepartitions (maybeSavePath: Option[String]) = {
    val kernel = (x: Seq[Double], y: Seq[Double]) =>
    (x zip y).
      map { case (x, y) => x - y }.
      foldLeft (0.0) { (a, x) => a + x*x }

    val manuscriptEmbeddings = pr (pc.getPathManuscriptsEmbedding)
    val manuscriptEmbeddingsEx = manuscriptEmbeddings.
      join (manuscripts, $"id" === $"eid").
      select ("issn", "year", "eid", "embedding").
      toDF ("issn", "manYear", "eid", "manEmbedding")
        
    val journalEmbeddings = pr (pc.getPathJournalsEmbeddingPerYear)
    val j1 = manuscriptEmbeddingsEx.
      join (journalEmbeddings, $"issn" === $"id" && $"year" < $"manYear").
      select ("eid", "issn", "year", "manYear", "embedding", "manEmbedding")

    val d = j1.map { x =>
      val manYear = x.getShort (3)
      val journalYear = x.getShort (2)
      val manEmbedding = x.getSeq[Double] (5)
      val journalEmbedding = x.getSeq[Double] (4)
      val d2 = kernel (manEmbedding, journalEmbedding)
      (x.getString(0), x.getString (1), manYear - journalYear, d2)
    }.toDF ("eid", "issn", "dy", "d2")
    val d1 = d.where ($"dy" === 1).select ("eid", "issn", "d2").toDF ("eid", "issn", "d2_1")
    val d2 = d.where ($"dy" === 2).select ("eid", "d2").toDF ("eid", "d2_2")
    val diff = d1.join (d2, "eid").select ($"eid", $"issn", ($"d2_2" - $"d2_1") as "d2_diff").where ($"d2_diff" > 0)
    val w = Window.orderBy ("d2_diff").partitionBy ("issn")
    val ptiles = diff.select ($"eid", $"issn", $"d2_diff", percent_rank().over (w).alias ("ptile"))
    maybeSavePath foreach {
      ptiles.write.mode ("overwrite") parquet _
    }
    ptiles
  }

  def loadTrendingReferenceForIssn (issn: String) = {
    val repartition = pr (pc getPathTrendingReference).
      where ($"issn" === issn).
      orderBy ("ptile").
      select ("d2_diff", "ptile").
      collect
    repartition.map { x => PercentileElement (x getDouble 0, x getDouble 1) }
  }

  def doitSaveTrendingRepartition = 
    computeTrendingRepartitions (Some (pc getPathTrendingReference))

}
 
