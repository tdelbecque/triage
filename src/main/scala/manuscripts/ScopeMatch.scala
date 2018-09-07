package com.sodad.els.triage.manuscripts
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.sodad.els.triage.config._

case class EmbeddingExtRecord (id: String, embedding: Seq[Double], issn: String, subjareas: Seq[String])

class ScopeMatchApp (config: EmbeddingAppConfig)
  (implicit session: SparkSession)
    extends EmbeddingApp (config) {
  import session.implicits._

  lazy val subjareasMap = 
    manuscripts.
      select ("issn", "subjareas").
      distinct.
      as[(String, Seq[String])].
      collect.
      map { x => (x._1, x._2 sorted) }.
      toMap

  val embeddings : DataFrame = pr (pc getPathManuscriptsEmbedding)
  val embeddingsExt : Dataset[EmbeddingExtRecord] = 
    embeddings.
      join (manuscripts, $"id" === $"eid").
      select ("id", "embedding", "issn", "subjareas").
      as[EmbeddingExtRecord]

  val journalEmbeddings : Dataset[EmbeddingRecord[Double]] = 
    computeJournalCentroidsFromManuscriptsEmbeddings (
      manuscripts,
      embeddings,
      config.embedding.dimension
    ).as[EmbeddingRecord[Double]]

  def scopeMatchContrastPairFromManuscriptId (eid: String) = {
    val candidatePaper = 
      manuscripts.
        where ($"eid" === eid).
        as[ManuscriptsContentRecord].
        head
    val candidatePaperIssn = candidatePaper.issn
    val candidatePaperSubjareas = candidatePaper.subjareas
    val candidatePaperJournalEmbedding = 
      journalEmbeddings.
        where ($"id" === candidatePaperIssn).
        head
    val paperEmbeddingsForCandidateSubjareas = 
      embeddingsExt.
        filter { x => 
          x.subjareas.intersect (candidatePaperSubjareas).size > 0
        }.
        select ("id", "embedding").
        as[EmbeddingRecord[Double]]
    val mostInScope = mostSimilars (
      Seq (candidatePaperJournalEmbedding embedding),
      paperEmbeddingsForCandidateSubjareas,
      5)
    Some (mostInScope)
  }
}
 
