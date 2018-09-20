package com.sodad.els.triage.manuscripts
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import com.sodad.els.triage.config._

case class EmbeddingExtRecord (
  id: String, 
  embedding: Seq[Double], 
  issn: String, 
  subjareas: Seq[String])

class ScopeMatch (
  val issn: String,
  val scores: Map[String, Double],
  val freqHistoRef: (Long, Map[String, (Long, Double)]),
  val freqHistoTest: (Long, Map[String, (Long, Double)]) ) 
{
  def value = scores.getOrElse (issn, 0.0)
}

class ScopeMatchApp (val embeddingApp : EmbeddingApp) {
  import embeddingApp._
  import session.implicits._

  def computeNormalRepartition (x: Double) = {
    import org.apache.commons.math3.special.Erf.erf
    0.5 * (1 + erf (x / Math.sqrt (2)))
  }

  def computeBinomVariance (p: Double, n: Double) = 
    p*(1.0 - p) / n
  
  def computeDeltaPropError (
    pReference: Double, nReference: Double, 
    pTest: Double, nTest: Double) =
    Math.sqrt (computeBinomVariance (pReference, nReference) + 
      computeBinomVariance (pTest, nTest))

  def computeScoreDelta (
    pReference: Double, nReference: Double, 
    pTest: Double, nTest: Double) = {
    val error = computeDeltaPropError (pReference, nReference, pTest, nTest)
    computeNormalRepartition ((pTest - pReference) / error)
  }

  def computeScore (
    pReference: Double, nReference: Double, 
    pTest: Double, nTest: Double) = {
    val s = computeScoreDelta (pReference, nReference, pTest, nTest)
    if (s > 0.5) 
      2.0 * (s - 0.5)
    else
      0.0
  }

  /** subject areas of issns */
  lazy val subjareasMap : Map[String, Set[String]] = 
    manuscripts.
      select ("issn", "subjareas").
      distinct.
      as[(String, Seq[String])].
      collect.
      foldLeft (Map.empty[String, Set[String]]) { case (a, (issn, subjareas)) =>
        a + (issn -> (a.getOrElse (issn, Set.empty[String]) ++ subjareas))
      }

  val embeddings : DataFrame = pr (pc getPathManuscriptsEmbedding)
  val embeddingsExt : Dataset[EmbeddingExtRecord] = 
    embeddings.
      join (manuscripts, $"id" === $"eid").
      select ("id", "embedding", "issn", "subjareas").
      as[EmbeddingExtRecord]

  val journalEmbeddings : Dataset[EmbeddingRecord[Double]] = 
    computeJournalCentroidsFromManuscriptsEmbeddings (
      manuscripts.toDF,
      embeddings,
      config.embedding.dimension,
      None
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

  def method1ForEmbedding (
    subEmbedding: Seq[Double], 
    targetIssn: String, 
    neighborhoodSize: Int, 
    remEid: String = "") : ScopeMatch = {
    /* gather in a map the nb of occurrences of each issns in
     * the argument dataset */ 
    def histo (data: Dataset[_]) = data.
      groupBy ("issn").
      count.
      as [(String, Long)].
      collect.
      toMap
    /* converts an count histogram to a frequency histogram */ 
    def freqHisto (h: Map[String, Long]): (Long, Map[String, (Long, Double)]) = {
      val N = h.foldLeft (0L) { case (a, (_, n)) => a + n }
      (N, h.mapValues { n => (n, n.toDouble / N) })
    }

    /* find all the papers that share at least one subject area 
     * with the targeted issn. They are the "slibings" */
    val targetSubjArea : Seq[String]= subjareasMap (targetIssn).toSeq
    val slibingsManuscripts = manuscripts.
      filter { ! _.subjareas.intersect (targetSubjArea).isEmpty }.
      filter { _.eid != remEid }
    val slibingsEmbeddings = manuscriptsEmbeddings.
      join (slibingsManuscripts, $"id" ===  $"eid").
      select ("id", "embedding").
      as[ManuscriptEmbeddingRecord]
    val referenceHistogram = histo (slibingsManuscripts)

    /* computes histograms in a neighborhood of the submitted paper */
    val neighborEids = mostSimilars (Seq (subEmbedding), slibingsEmbeddings, neighborhoodSize).head
    val neighborEidsDataframe = sc.parallelize (neighborEids).toDF ("eid", "distance")
    val neighborManuscripts = slibingsManuscripts.join (neighborEidsDataframe, "eid")
    val neighborHistogram = histo (neighborManuscripts)

    val (nRef, freqHistoRef) = freqHisto (referenceHistogram)
    val (nTest, freqHistoTest) = freqHisto (neighborHistogram)

    val scores = freqHistoTest.map { case (issn, (n, pTest)) =>
      val pRef = freqHistoRef (issn)._2
      val score = computeScore (
        pReference = pRef,
        nReference = nRef,
        pTest = pTest,
        nTest = nTest)
      (issn, score)
     }.toMap
    
    new ScopeMatch (
      targetIssn, scores, 
      freqHistoRef = freqHisto (referenceHistogram), 
      freqHistoTest = freqHisto (neighborHistogram))
  }

  /** First method of Scope Match dimension extraction
    * It is based on comparing the histograms of journals for given 
    * subject areas, computed on the manuscripts dataset on one hand and
    * inside a neighborhood of the submitted paper on the other hand. 
    *
    */
  def method1ForEid_0 (subEid: String, targetIssn: String, neighborhoodSize: Int) = {
    /* gather in a map the nb of occurrences of each issns in
     * the argument dataset */ 
    def histo (data: Dataset[_]) = data.
      groupBy ("issn").
      count.
      as [(String, Long)].
      collect.
      toMap
    /* converts an count histogram to a frequency histogram */ 
    def freqHisto (h: Map[String, Long]): (Long, Map[String, (Long, Double)]) = {
      val N = h.foldLeft (0L) { case (a, (_, n)) => a + n }
      (N, h.mapValues { n => (n, n.toDouble / N) })
    }

    /* find all the papers that share at least one subject area 
     * with the targeted issn. They are the "slibings" */
    val targetSubjArea : Seq[String]= subjareasMap (targetIssn).toSeq
    val slibingsManuscripts = manuscripts.
      filter { ! _.subjareas.intersect (targetSubjArea).isEmpty }.
      filter { _.eid != subEid }
    val slibingsEmbeddings = manuscriptsEmbeddings.
      join (slibingsManuscripts, $"id" ===  $"eid").
      select ("id", "embedding").
      as[ManuscriptEmbeddingRecord]
    val referenceHistogram = histo (slibingsManuscripts)

    /* computes histograms in a neighborhood of the submitted paper */
    val subEmbedding = manuscriptsEmbeddings.filter ($"id" === subEid).head.embedding
    val neighborEids = mostSimilars (Seq (subEmbedding), slibingsEmbeddings, neighborhoodSize).head
    val neighborEidsDataframe = sc.parallelize (neighborEids).toDF ("eid", "distance")
    val neighborManuscripts = slibingsManuscripts.join (neighborEidsDataframe, "eid")
    val neighborHistogram = histo (neighborManuscripts)

    val (nRef, freqHistoRef) = freqHisto (referenceHistogram)
    val (nTest, freqHistoTest) = freqHisto (neighborHistogram)

    val scores = freqHistoTest.map { case (issn, (n, pTest)) =>
      val pRef = freqHistoRef (issn)._2
      val score = computeScore (
        pReference = pRef,
        nReference = nRef,
        pTest = pTest,
        nTest = nTest)
      (issn, score)
     }.toMap
    
    (scores, freqHisto (referenceHistogram), freqHisto (neighborHistogram))
  }

  def method1ForEid (subEid: String, targetIssn: String, neighborhoodSize: Int) = {
    val subEmbedding = manuscriptsEmbeddings.filter ($"id" === subEid).head.embedding
    method1ForEmbedding (subEmbedding, targetIssn, neighborhoodSize, subEid)
  }

  def method1ForText (text: String, targetIssn: String, neighborhoodSize: Int) = {
    val subEmbedding = computeTextEmbedding (Seq (text))(0)
    method1ForEmbedding (subEmbedding, targetIssn, neighborhoodSize)
  }
}

