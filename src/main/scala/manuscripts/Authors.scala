package com.sodad.els.triage.authors

import org.apache.spark.sql.{SparkSession, Dataset, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.sodad.els.triage.config._
import com.sodad.els.triage.manuscripts._
import com.sodad.els.triage.utils._

case class HIndexRecord (auid: String, hindex: Long)
case class FirstPublicationRecord (auid: String, first_published: String)

object AuthorsApp {
  val firstpub_path_initial =
    "s3://wads/epfl/data/author_metrics/scopus_first_publication_20180707"

  val hindex_path_initial = 
    "s3://wads/epfl/data/author_metrics/h_index_20180707"

  val hindex_paths = Seq (
    "s3://wads/epfl/data/author_metrics/h_index_20180102",
    "s3://wads/epfl/data/author_metrics/h_index_20180707",
    "s3://wads/epfl/data/author_metrics/h_index_20180801")
}

class AuthorsApp (pc: PersistConfig) (implicit session: SparkSession) {
  import session.implicits._
  import AuthorsApp._

  lazy val hindexes = 
    session.read.parquet (hindex_path_initial).
      toDF ("auid", "hindex").
      dropDuplicates.
      as[HIndexRecord]

  lazy val firstPublications =
    session.read.parquet (firstpub_path_initial).
      dropDuplicates.
      as[FirstPublicationRecord]

  def saveInitialHIndexes (path: String) = 
    hindexes.write.mode ("overwrite").
      option ("path", path) save

  def saveInitialFirstPublications (path: String) =
    firstPublications.write.mode ("overwrite").
      option ("path", path) save

  def buildHIndexFile (
    maybeSavePath: Option[String], 
    paths: String*) : Dataset[HIndexRecord] = {
    val w = Window.
      partitionBy ("auid").
      orderBy ($"src".desc, $"hindex".desc)
    val res = 
      paths.
        zipWithIndex.
        map { case (p, i) =>
          session.read.parquet (p).as[(String, Long)].map { r =>
            (r._1, r._2, i)
          }.toDF ("auid", "hindex", "src")
        }.
        reduceLeft { (a, b) => a union b }.
        select ($"auid", $"hindex", $"src", rank().over (w) alias "rnk" ).
        where ($"rnk" === 1).
        select ("auid", "hindex")
    maybeSavePath foreach { res.write.mode ("overwrite").parquet (_) }
    res.as[HIndexRecord]
  }

  def doitSaveHIndex =
    buildHIndexFile (Some (pc getPathHIndex), hindex_paths: _*)

  def extractIssnAuthorPairs (manuscripts: DataFrame) =
    manuscripts.
      select ("eid", "issn", "au").as[PaperAuthorsRecord].
      flatMap { case PaperAuthorsRecord (eid, issn, au) =>
        au map { x => (issn, x.auid) }
      }.
      toDF ("issn", "auid").
      as[(String, String)].
      where { $"issn" isNotNull }.
      distinct
            
  def extractHIndexRepartitionPerJournal (
    manuscripts: DataFrame, 
    hIndexDF: DataFrame, 
    maybeSavePath: Option[String]) = {
    val pairs = extractIssnAuthorPairs (manuscripts toDF)
    val j = pairs.join (hIndexDF, "auid")
    val w = Window.partitionBy ("issn").orderBy ($"hindex")
    val ptile =
      j.
        select ($"issn", $"hindex", percent_rank().over (w).alias ("ptile")).
        distinct
    maybeSavePath foreach {
      ptile.write.mode ("overwrite").parquet (_)
    }
    ptile
  }
    
  def doitSaveHIndexRepartitionPerJournal (manuscripts: DataFrame) = {
    val hIndex = session.read.parquet (ElsStdEmbeddingConfig.persist.getPathHIndex)
    extractHIndexRepartitionPerJournal (
      manuscripts, 
      hIndex, 
      Some (pc.getPathHIndexRepartitionPerJournal))
  }
        
  def loadHIndexRepartitionPerJournal (
    path: String = pc.getPathHIndexRepartitionPerJournal
  ) = 
    session.read.parquet (path).
      as[(String, Long, Double)].
      map { x => (x._1, x._2.toDouble, x._3) }.
      toDF ("issn", "hindex", "ptile").
      orderBy ("issn", "ptile")

  def loadHIndexRepartitionForIssn (
    issn: String, 
    maybeRepartitionData: Option[Dataset[_]] = None
  ) : Seq[PercentileElement] = {
    val repartitionData = 
      maybeRepartitionData.getOrElse (loadHIndexRepartitionPerJournal ())
    val repartition = 
      repartitionData.
        where ($"issn" === issn).
        orderBy ("ptile").
        select ("hindex", "ptile").
        collect
    repartition.map { x => PercentileElement (x getDouble 0, x getDouble 1) }
  }

  def academicAuthorityForIssn (issn: String) = {
    val hindex = 
      session.read.parquet (pc.getPathHIndex).
        as[(String, Double)].
        collect.
        toMap

    val normalizer = new PTileNormalizer (
      loadHIndexRepartitionForIssn (issn)
    )

    { eid : String => hindex.get (eid).map (normalizer) }
  }
}
