package com.sodad.els.triage.references

import org.apache.spark.sql.SparkSession

case class ReferenceRecord (
  datesort: String, 
  issn: String, 
  eid: String, 
  reference: String
)

object ReferencesApp {
  val path_initial = 
    "s3://wads/epfl/data/author_metrics/scopus_references"
}

class ReferencesApp (implicit session: SparkSession) {
  import session.implicits._
  import ReferencesApp._

  lazy val references = 
    session.read.parquet (path_initial).
      toDF ("datesort", "issn", "eid", "reference").
      dropDuplicates.
      as[ReferenceRecord]

  def saveInitialReferences (path: String) =
    references.write.mode ("overwrite").
      option ("path", path) save
}
