package com.sodad.els.triage.authors

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib._
import com.sodad.els.triage.manuscripts._
import com.sodad.els.triage.config._

class CoAuthorsApp (
  manuscripts: Dataset[ManuscriptRecord], 
  pc: PersistConfig
) (implicit session: SparkSession) {
  import session.implicits._

  def getManuscriptsAuthors 
    (data: Dataset[ManuscriptRecord] = manuscripts) =
    data.flatMap {x =>
      x.au map { y => (
        x eid,
        y auid,
        y orcid,
        y given_name,
        y given_name_pn,
        y surname,
        y surname_pn,
        y nametext,
        y indexed_name,
        y e_address_type) } }.
      toDF ("eid", "auid", "orcid", "given_name", "given_name_pn",
        "surname", "surname_pn", "nametext", "indexed_name",
        "e_address_type")

  def getAuthorsAmbiguity (maybeSavePath: Option[String] = None) : Dataset[(String, Long)] = {
    val ret = getManuscriptsAuthors (manuscripts).
      select ("auid", "indexed_name").
      distinct.groupBy ("auid").
      count.
      select ($"auid", ($"count" - 1) as "ambiguity").
      as[(String, Long)]

    maybeSavePath foreach { ret.write.mode ("overwrite") parquet _ }
    ret
  }
    
  lazy val savedAuthorsAmbiguity = 
    session.read.parquet (pc.getPathAuthorsAmbiguity).as[(String, Long)]

  def doitSaveAuthorsAmbiguity =
    getAuthorsAmbiguity (Some (pc.getPathAuthorsAmbiguity))

  def getUnambiguousAuthors (
    ambiguity: Long = 1, 
    maybeAuthorsAmbiguity: Option[Dataset[(String, Long)]] = None
  ) : Dataset[String] = {
    maybeAuthorsAmbiguity.
      getOrElse (getAuthorsAmbiguity ()).
      where ($"ambiguity" <= ambiguity).
      select ("auid").
      as[String]
  }

  def getUniqueAuthorIds 
    (data: Dataset[ManuscriptRecord] = manuscripts) =
    data.flatMap { x =>
      x.au map { y => (x eid, y auid) }
    }.toDF ("eid", "auid")

  def getManuscriptsStartingFromISSN (issns: String*) : 
      Dataset[ManuscriptRecord] = {
    val rootAuthors = 
      manuscripts.
        filter (issns contains _.issn).
        flatMap { x => x.au map (_ auid) }.
        distinct.
        toDF ("auid")
        
    val allFlattenAuthors = 
      manuscripts.
        flatMap { x => x.au map { y => (x eid, y auid) } }.
        toDF ("eid", "auid")

    val generatedEids =
      allFlattenAuthors.
        join (rootAuthors, "auid").
        select ("eid").
        distinct
    manuscripts.join (generatedEids, "eid").as[ManuscriptRecord]
  }
    
  def coauthors (
    data: Dataset[ManuscriptRecord], 
    ambiguity: Long, 
    deduplicatePairs: Boolean
  ) : Dataset[(String, String)] = {
    val unambiguousAuthorsId = getUnambiguousAuthors (ambiguity)
    val flattenAuthors =
      data.
        flatMap { x => x.au map { y => (x eid, y auid) } }.
        toDF ("eid", "auid").
        join (unambiguousAuthorsId, "auid")
                
    val coAuthorsPairs =
      flattenAuthors.
        toDF ("left", "eid").
        join (flattenAuthors.toDF ("right", "eid"), "eid").
        select ("left", "right").
        filter { x => x.getString(0) != x.getString (1) }
                
    { 
      if (deduplicatePairs)
        coAuthorsPairs distinct
      else
        coAuthorsPairs 
    }.as[(String, String)]
  }
    
  def extractCoAuthorsWithManuscripts (
    data: Dataset[ManuscriptRecord], 
    ambiguity: Long, 
    maybeAuthorsAmbiguity: Option[Dataset[(String, Long)]] = None,
    maybeSavePath: Option[String] = None
  ) : Dataset[(String, String, String, String)] = {
    val unambiguousAuthorsId = getUnambiguousAuthors (ambiguity, maybeAuthorsAmbiguity)
    val flattenAuthors =
      data.
        flatMap { x =>
          x.au map { y => (x eid, x issn, y auid) }
        }.
        toDF ("eid", "issn", "auid").
        join (unambiguousAuthorsId, "auid")
                
    val ret = flattenAuthors.
      toDF ("left", "eid", "issn").
      join (flattenAuthors.toDF ("right", "eid", "issnr"), "eid").
      select ("eid", "issn", "left", "right").
      filter { x => x.getString(2) != x.getString (3) }.
      as[(String, String, String, String)]

    maybeSavePath foreach {
      ret.write.mode ("overwrite") parquet _
    }
    ret
  }

  def doitSaveCoAuthorsWithManuscripts =
    extractCoAuthorsWithManuscripts (
      manuscripts,
      ambiguity = 1,
      maybeAuthorsAmbiguity = Some(savedAuthorsAmbiguity),
      maybeSavePath = Some (pc.getPathCoAuthorsWithManuscripts))

  def getUniqueAuthorsIdsStartingFromISSN (issns: String*) =
    getUniqueAuthorIds (manuscripts)
    
  def createCoAuthorsPairs (
    data: Dataset[ManuscriptRecord], 
    ambiguity: Long, 
    deduplicatePairs: Boolean, 
    maybeSavePath: Option[String]
  ) : Dataset[(String, String)] = {
    val authorPairs = coauthors (data, ambiguity, deduplicatePairs)
    maybeSavePath foreach { 
      authorPairs.write.mode ("overwrite").option ("path", _) save 
    }
    authorPairs
  }
    
  def _createCoAuthorsPairsForISSN (
    ambiguity: Long, 
    deduplicatePairs: Boolean, 
    savePathPrefix: String, 
    issns: String*
  ) = {
    val fullCoAuthorsSavePath = 
      s"${savePathPrefix}coauthorsWithManuscripts"
    extractCoAuthorsWithManuscripts (
      getManuscriptsStartingFromISSN (issns:_*), 
      ambiguity).
      write.
      mode ("overwrite").
      parquet (fullCoAuthorsSavePath)

    val fullCoAuthorsData = session.read parquet fullCoAuthorsSavePath
    issns foreach { issn =>
      val d = fullCoAuthorsData.
        where ($"issn" === issn).
        select ("left", "right")
      val x = if (deduplicatePairs)
        d.distinct
      else
        d
      x.
        write.
        mode ("overwrite").
        option ("path", s"$savePathPrefix$issn") save } 
  }

  def extractCoJournals (ambiguity: Long) = {
    val unambiguousAuthorsId = getUnambiguousAuthors (ambiguity, Some (savedAuthorsAmbiguity))
    val data = manuscripts.flatMap {x =>
      x.au map { y => (x issn, y auid ) } }.
      toDF ("issn", "auid").
      join (unambiguousAuthorsId, "auid").
      distinct

    val co =
      data.toDF ("auid", "left").join (data.toDF ("auid", "right"), "auid")
    co.groupBy ("left", "right") count
  }

  def extractPageRankRepartitionForISSN (
    issn: String, 
    ambiguity: Long, 
    deduplicatePairs: Boolean, 
    convergence: Double = 0.01) = {
    val pairs = coauthors (
      getManuscriptsStartingFromISSN (issn), 
      ambiguity, deduplicatePairs
    ) persist
    val edges = pairs.map { x => Edge (x._1.toLong, x._2.toLong, null) }
    val vertices = pairs.map { x => (x._1.toLong, null) }
    pairs unpersist
    val g = Graph (vertices.rdd, edges.rdd) cache
    val pageRankGraph = PageRank.runUntilConvergence (g, convergence)
    pageRankGraph.vertices.toDF ("auid", "pageRank")
  }

}
