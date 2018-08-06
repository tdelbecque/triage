package com.sodad.els.triage.manuscripts

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import com.sodad.els.triage.config._

case class AuthorRecord (
    authorSeq: Int,
    auid: String,
    author_type: String,
    collaboration: String,
    degrees: String,
    e_address_type: String,
    given_name: String,
    given_name_pn: String,
    indexed_name: String,
    initials: String,
    initials_pn: String,
    nametext: String,
    orcid: String,
    suffix: String,
    surname: String,
    surname_pn: String
)

case class AffiliationRecord (
    affiliation_address_part: String,
    affiliation_city: String,
    affiliation_city_group: String,
    affiliation_country: String,
    affiliation_ids: Seq[String],
    affiliation_organization: Seq[String],
    affiliation_organization_count: Int,
    affiliation_postal_code: String,
    affiliation_state: String,
    affiliation_tag_country: String,
    affiliation_text: String,
    afid: String,
    dptid: String    
)

case class AuthorAffiliationRecord (Authorseq: Int, affiliation_seq: Int, validity_B: Boolean)
    
case class ManuscriptTermRecord (candidate: String, controlled: String, mainterm: String, `type`: String, weight: String)

case class ManuscriptSourceRecord (
    article_number: String, 
    country: String, 
    date_day: String, 
    date_month: String, 
    date_text: String, 
    date_year: String, 
    e_address: String, 
    e_address_type: String, 
    isbn_electronic: Seq[String], 
    isbn_other: Seq[String], 
    isbn_print: Seq[String], 
    issn_electronic: Seq[String], 
    issn_other: Seq[String], 
    issn_print: Seq[String], 
    publishername: String, 
    sourcetitle: String, 
    sourcetitle_abbrev: String, 
    srcid: String, 
    `type`: String, 
    website: String
)

case class ManuscriptRecord (
    datesort: String, 
    eid: String, 
    doi: String, 
    issn: String, 
    title: String, 
    abstr: String, 
    au: Seq[AuthorRecord], 
    af: Seq[AffiliationRecord], 
    au_af: Seq[AuthorAffiliationRecord], 
    citations: Seq[String], 
    keywords: Seq[String], 
    terms: Seq[ManuscriptTermRecord], 
    asjc: Seq[String], 
    subjareas: Seq[String], 
    source: ManuscriptSourceRecord, 
    copyright_types: String
)

case class ManuscriptsContentRecord (eid: Long, issn: String, title: String, 
  abstr: String, subjareas: Seq[String],
   publishername: String, sourcetitle: String, sourcetitle_abbrev: String)

case class PaperAuthorsRecord (eid: String, issn: String, au: Seq[AuthorRecord])
case class PaperAuthorRecord (eid: String, issn: String, auid: String)

object ManuscriptsApp {
  val dataRepositoryPrefix = "s3://wads/epfl"
  val path_initial = s"$dataRepositoryPrefix/data/scopus_manuscripts"
  val path_extra = s"$dataRepositoryPrefix/data/scopus_manuscripts_4_20180707"
  val path_authors = s"$dataRepositoryPrefix/thy/paperAuthors"
//  val path_manuscripts = s"$dataRepositoryPrefix/thy/manuscripts-content-valid-with-sources"

  val initialFieldNames = Seq("datesort", "eid", "doi", "issn", "title", "abstr", 
    "au", "af", "au_af", "citations", "keywords", "terms", "asjc", "subjareas", 
    "source", "copyright_types")

  def mkS3path (path: String) = s"$dataRepositoryPrefix/$path"

  def loadInitialData (path: String = path_initial) (implicit session: SparkSession) 
  : Dataset[ManuscriptRecord] = {
    import session.implicits._
    session.read.parquet (path).toDF (initialFieldNames:_*).as[ManuscriptRecord]
  }
  
  def loadExtraData (implicit session: SparkSession)
  : Dataset[ManuscriptRecord] = {
    import session.implicits._
    session.read.parquet (path_extra).toDF (initialFieldNames:_*).as[ManuscriptRecord]
  }

  val manuscriptsContentFieldNames = Seq ("eid", "issn", "title", "abstr", "subjareas", 
    "publishername", "sourcetitle", "sourcetitle_abbrev")

  /** Extract these fields from the initial manuscripts dataset:
    *   eid, issn, title, abstr, subjareas, publishername, sourcetitle, sourcetitle_abbrev
    * 
    * It keeps only those records for which `eid`, `issn`, `title`, `abstr`, `subjareas` 
    *  and `source` are not null.
    * 
    * @param data: the dataframe from where extraction is processed
    * @param maybeSavePath: if not `None`, where to save the extracted data. */
  def manuscriptsExtractContent (data: Dataset[ManuscriptRecord], maybeSavePath: Option[String] = None)
    (implicit session: SparkSession) = {
    import session.implicits._
    import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema => GR}
    val ret = 
      data.toDF.
        filter ($"eid".isNotNull && $"issn".isNotNull && $"title".isNotNull &&
          $"abstr".isNotNull && $"subjareas".isNotNull && $"source".isNotNull ).
        select ("eid", "issn", "title", "abstr", "subjareas", "source").
        map { x =>
          val source: GR = x.getAs[GR](5)
          (
            s"${x getLong 0}",
            x getString 1,
            x getString 2,
            x getString 3,
            x getSeq[String] 4,
            source getString 14,
            source getString 15,
            source getString 16
          ) }.
        toDF (manuscriptsContentFieldNames:_*).
        dropDuplicates
    maybeSavePath foreach { path =>
      ret.write.mode ("overwrite").option ("path", path) save }
    ret
  }


}

class ManuscriptsApp (val config: PersistConfig) (implicit session: SparkSession) {
  import session.implicits._
  import ManuscriptsApp._

  /** Initial manuscripts dataframe */
  lazy val manuscripts = loadInitialData ()

  /** manuscripts data for these issn's: 
    *  - 0003-2670 (Analytica Chimica Acta)
    *  - 0921-5093 (Materials Science and Engineering: A)
    *  - 2211-1247 (Cell Reports)
    *  - 0092-8674 (Cell)  */
  lazy val extra = loadExtraData

  lazy val paperAuthorsDataset : Dataset[PaperAuthorRecord] = 
    session.read.parquet (path_authors).as[PaperAuthorRecord]

  /** Extract ready to use content dataset */
  def doitExtractManuscriptsContent (path: String = config getPathManuscripts) =
    manuscriptsExtractContent (manuscripts, Some (path))
        
  /** Extract ready to use authors dataset */
  def doitExtractPaperAuthorsDataset (path: String = path_authors) = {
    val paperAuthorsDataset : Dataset[PaperAuthorRecord] =
      manuscripts.
        select ("eid", "issn", "au").as[PaperAuthorsRecord].
        flatMap { case PaperAuthorsRecord (eid, issn, au) =>
          au map { x => PaperAuthorRecord (eid, issn, x.auid) }
        }
    paperAuthorsDataset.write.mode ("overwrite").option ("path", path) save
  }
    
  /** Extracts all needed ready to use daatasets */
  def doit = {
    doitExtractManuscriptsContent ()
    doitExtractPaperAuthorsDataset ()
  }

  def journalDistribution (data: DataFrame) = {
    val N : Long = data.count
    data.select ("sourcetitle").groupBy ("sourcetitle").count.
      sort ($"count".desc).
      select ($"sourcetitle", $"count", ($"count"*100.0)/N as "pct" )
  }

  /** Computes the histograme of the subject areas in a dataframe, 
    *  provided this dataframe has a `subjareas` field.
    *  @return a ("subjarea", "count") DF. */
  def subjareasDistribution (data: DataFrame) =
    data.select ("subjareas").flatMap { x =>
      x.getSeq[String](0).toList
    }.
      toDF ("subjectarea").
      groupBy ("subjectarea").count.
      sort ($"count".desc)
}
