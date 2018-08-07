package com.sodad.els.triage.authors

import org.apache.spark.sql.SparkSession

case class HIndexRecord (auid: String, hindex: Long)
case class FirstPublicationRecord (auid: String, first_published: String)

object AuthorsApp {
  val firstpub_path_initial =
    "s3://wads/epfl/data/author_metrics/scopus_first_publication_20180707"

  val hindex_path_initial = 
    "s3://wads/epfl/data/author_metrics/h_index_20180707"
}

class AuthorsApp (implicit session: SparkSession) {
  import session.implicits._
  import AuthorsApp._

  lazy val hindexes = 
    session.read.parquet (hindex_path_initial).
      toDF ("auid", "hindex").
      as[HIndexRecord]

  lazy val firstPublications =
    session.read.parquet (firstpub_path_initial).
      as[FirstPublicationRecord]
}
