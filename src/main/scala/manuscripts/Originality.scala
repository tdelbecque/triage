package com.sodad.els.triage.dimensions

import org.apache.spark.sql.{SparkSession, Dataset}
import com.sodad.els.triage.manuscripts._
import com.sodad.els.triage.config._
import com.sodad.els.triage.utils._

case class ManuscriptsAuthorsSeqRecord (
  eid: String, 
  issn: String, 
  subjareas: Seq[String], 
  year: Short, 
  month: Short, 
  au: Seq[AuthorRecord] )

case class ManuscriptsAuthorsRecord (
  eid: String, 
  issn: String, 
  subjareas: Seq[String], 
  year: Short, 
  month: Short, 
  auid: String )

class OriginalityApp (
  manuscriptsApp: ManuscriptsApp, 
  embeddingApp: EmbeddingApp
) (implicit session: SparkSession) {
  import session.implicits._

  def extractAuthors (
    data: Dataset[ManuscriptRecord] = manuscriptsApp.manuscripts, 
    maybeSavePath: Option[String] = None
  ) (implicit session: SparkSession) : Dataset[ManuscriptsAuthorsRecord] = {
    val datePattern = 
      "^(201[5-9])(0[1-9]|1[012])(0[1-9]|[12][0-9]|3[01])$".r
    val ret =
      data.
        filter { x =>
          x.eid != null && x.issn != null && x.title != null &&
          x.abstr != null && x.subjareas != null && x.source != null &&
          x.datesort != null && 
          datePattern.findFirstIn (x.datesort).size == 1 }.
        map { x =>
          val datePattern (y, m, d) = x.datesort
          ManuscriptsAuthorsSeqRecord (
            eid = x.eid,
            issn = x issn,
            subjareas = x subjareas,
            au = x au,
            year = y toShort,
            month = m toShort ) }.
        flatMap { x =>
          x.au map { y =>
            ManuscriptsAuthorsRecord (
              eid = x eid,
              issn = x issn,
              subjareas = x subjareas,
              year = x year,
              month = x month,
              auid = y auid )
          } }.
        dropDuplicates ("eid", "auid")
        
    maybeSavePath foreach { path =>
      ret.write.mode ("overwrite").option ("path", path) save }
    ret
  }
  
  def extractPairEmbeddings (
    authPapers: Dataset[ManuscriptsAuthorsRecord] = extractAuthors ()
  ) = {
    val authPapersAsc = 
      authPapers.toDF ("eid_asc", "issn_asc", "subjareas_asc", 
        "year_asc", "month_asc", "auid_asc")
    val j = authPapers.join (
      authPapersAsc, 
      $"auid" === $"auid_asc" && 
        ($"year" >= $"year_asc" || 
          ($"year" >= $"year_asc" && $"month" > $"month_asc")))

    val withOrigEmb = 
      j.
        select ("eid", "issn", "eid_asc").
        dropDuplicates.
        join (embeddingApp.manuscriptsEmbeddings, $"id" === $"eid").
        drop ("id")

    withOrigEmb.
      join (embeddingApp.manuscriptsEmbeddings.
        toDF ("id", "embedding_asc"), $"id" === $"eid_asc").
      drop ("id")
  }
  
  def extractPairDistances (
    pairsDF: Dataset[_] = extractPairEmbeddings (),
    kernel: Kernels.K[Double, Double] = Kernels.d2
  ) = {
    val kernel_ = (x: Seq[Double], y: Seq[Double]) =>
    (x zip y).
      map { case (x, y) => x - y }.
      foldLeft (0.0) { (a, x) => a + x*x }

    pairsDF.
      select ("eid", "issn", "eid_asc", "embedding", "embedding_asc").
      map { x =>
        val embedding = x getSeq[Double] 3
        val embeddingAsc = x getSeq[Double] 4
        val d2 = kernel (embedding, embeddingAsc)
        (x getString 0, x getString 1, x getString 2, d2 )
      }.
      toDF ("eid", "issn", "eid_asc", "d2")
  }
}

