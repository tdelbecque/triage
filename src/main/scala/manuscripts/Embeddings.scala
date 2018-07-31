package com.sodad.els.triage.manuscripts

import scala.collection.JavaConversions._
import java.util.Properties
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling.CoreAnnotations._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql._
import org.apache.spark.mllib.feature.{Word2Vec, Word2VecModel}
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.types.StringType

trait Transformer extends java.io.Serializable {
  def transform (term: String) : String
}

class LowerCaseTransformer extends Transformer {
  def transform (term: String) = term toLowerCase
}

case class TokenizedRecord (
  id: String,
  terms: Seq[String])
 
/** a manuscript (identified by id), transformed as a weighed BoW. */
case class WeightedTermRecord (
    id: String,
    weights: Map[String, Double])

case class WordEmbeddingRecord (word: String, embedding: Seq[Float])

case class OHEClassificationModelPerformancesRecord (
    label: String,
    fitcount: Long,
    totalCount: Long,
    accuracy: Double
)

package object replacement {
  val defaultReplacementPatterns = {
    val floatStr = """(?:\+/-|\+-|\+|-)?\s*\d+(?:\.\d+)*"""
    List (
      s"$floatStr\\s*%".r -> " sodadpct ",
      s"[<>=]+\\s*$floatStr".r -> " sodadnumexpr ",
      s"$floatStr\\s*[<>=]+".r -> " sodadnumexpr ",
      s"$floatStr".r -> " sodadnum ",
      """\(.*?\)""".r -> " ",
      """\s+""".r -> " ")
  }

  def replacePatternsInRecord (r: (String, String),
    patterns: Seq[(scala.util.matching.Regex, String)] = defaultReplacementPatterns)
      : (String, String) =
    ( r._1,
      patterns.foldLeft (r._2) { case (a, (e, r)) => e replaceAllIn (a, r) } )

  def replacePatterns(data: Dataset[(String, String)],
    patterns: Seq[(scala.util.matching.Regex, String)] = defaultReplacementPatterns,
    maybeSavePath: Option[String] = None)
    (implicit session: SparkSession): Dataset[(String, String)] = {
    import session.implicits._
    val replacement = data map (replacePatternsInRecord(_, patterns))
    maybeSavePath foreach { path =>
      replacement.write.option ("path", path) save
    }
    replacement
  }
  
}

object EmbeddingApp {
  val path_manuscripts = ManuscriptsApp.path_manuscripts
  val dataRepositoryPrefix = ManuscriptsApp.dataRepositoryPrefix

  val path_abstractsTokenized = s"$dataRepositoryPrefix/thy/manuscripts-abstracts-tokenized"
  val path_titlesTokenized = s"$dataRepositoryPrefix/thy/manuscripts-titles-tokenized"
  val path_tokensTotalFrequencies = s"$dataRepositoryPrefix/thy/manuscripts-tokens-total-frequencies"
  val path_tokensDocFrequencies = s"$dataRepositoryPrefix/thy/manuscripts-tokens-doc-frequencies"
  val path_tokensIdfWeights = s"$dataRepositoryPrefix/thy/manuscripts-tokens-idf-weights"
  val path_tfidfWeights = s"$dataRepositoryPrefix/thy/manuscripts-tfidf-weights"
  val path_wordEmbeddingOnAbstracts = s"$dataRepositoryPrefix/thy/wordEmbeddingOnAbstracts"
  val path_manuscriptsEmbedding = s"$dataRepositoryPrefix/thy/manuscriptsEmbedding"
  val path_areaOneHotEncodingForClassification = s"$dataRepositoryPrefix/thy/areaOHE4classifData"
  val path_areaOneHotEncodingIndex = s"$dataRepositoryPrefix/thy/areaOHEIndex"
  val path_areaOneHotEncodingModelPerformances = s"$dataRepositoryPrefix/thy/areaOHEModelPerformances"

  def tokenizeTextPerSentence (texts: Seq[String], maybeTransformer: Option[Transformer] = None) = {
    val transform: String => String =
      maybeTransformer.
        map { _.transform _ }.
        getOrElse { x => x }
    val props = new Properties ()
    props put ("annotators", "tokenize, ssplit")
    val pipeline = new StanfordCoreNLP (props)
    texts.map { t =>
      val ann = new Annotation (t)
      pipeline annotate ann
        (for (s <- ann get classOf[SentencesAnnotation])
        yield (
          (for (t <- s.get (classOf[TokensAnnotation]).map { x => transform (x.originalText()) }) yield t) toList
        )).toList.flatMap {x => x}
    }
  }

}

class EmbeddingApp (implicit session: SparkSession) {
  import EmbeddingApp._
  import session.implicits._

  val pr = session.read.parquet _
  val sc = session.sparkContext

  lazy val manuscripts = session.read.parquet(path_manuscripts)
  lazy val titles = manuscripts.select ("title").map (_.getString (0))
  lazy val abstracts = manuscripts.select ("abstr").map (_.getString (0))

  lazy val flattenSubjAreas =
    manuscripts.select ("eid", "subjareas").flatMap { x =>
      val eid = x getString 0
      val areas: Seq[String] = x getSeq[String] 1
      areas.map { (eid, _) }
    }.
      toDF ("eid", "area")
  
  lazy val areaFrequencies =
    flattenSubjAreas.
      groupBy ("area").count.
      select ($"area", $"count" as "freq", ($"count" * 100.0 / manuscripts.count) as "pct").
      orderBy ($"count" desc)

  /** Given a dataframe of `(String, String)` where `_1` is an id, and `_2` is the text to
    *  tokenize, creates a record for each sentence in `_2`.
    *  @param data: what to tokenize
    *  @param maybeTransformer: if not `None`, a tranformer that will be applied to each token
    *  @param maybeSavePath: where to write the new dataset */
  def tokenizePerSentence (
    data: Dataset[(String, String)],
    maybeTransformer: Option[Transformer] = None,
    maybeSavePath: Option[String]
  ) : Dataset[TokenizedRecord] = {
    val transform : String => String = maybeTransformer match {
      case Some (s) => s.transform _
      case _ => x => x
    }

    val tokenized = data mapPartitions { it =>
      val props = new Properties ()
      props put ("annotators", "tokenize, ssplit")
      val pipeline = new StanfordCoreNLP (props)
      it flatMap { case (id, payload) =>
        val ann = new Annotation (payload)
        pipeline annotate ann
        (for (s <- ann get classOf[SentencesAnnotation]) yield (
          for (t <- s.get (classOf[TokensAnnotation]).map { x =>
            transform (x.originalText ())})
          yield t).toList).map { TokenizedRecord (id, _) }.toList
      }
    }
    maybeSavePath foreach ( tokenized.write.mode ("overwrite").option ("path", _).save )
    tokenized
  }

  def saveTokenizedAbstracts (data: DataFrame, pathToSave: String) = {
    val abstracts = data.select ("eid", "abstr").
      map {x => (x getString 0, x getString 1)}
    val abstractsReplace = replacement.replacePatterns (abstracts).map {x => (x._1, x._2.toLowerCase)}
    val tokens = tokenizePerSentence (abstractsReplace, None, None).map (_ terms)
    tokens.write.option ("path", pathToSave) save
    }

  /** Helper function: 
    */
  def extractVocabularyHelper (
    data: Dataset[Seq[String]], 
    seqop: (Map[String, Int], Seq[String]) => Map[String, Int]) = {
    val combop : (Map[String, Int], Map[String, Int]) => Map[String, Int] =
      (a, b) => b.foldLeft (a) { case (a, v) =>
        a + (v._1 -> (a.getOrElse (v._1, 0) + v._2))
      }

    data.rdd.aggregate (Map.empty[String, Int])(seqop, combop)
  }

  /** Given a dataset of sequences of tokens, count the nb of times each term is found */ 
  def extractVocabularyFrequencyForTokenized (data: Dataset[Seq[String]]) = {
    val seqop : (Map[String, Int], Seq[String]) => Map[String, Int] =
      (a, xs) => xs.foldLeft (a) { (a, x) =>
        a + (x -> (a.getOrElse (x, 0) + 1))
      }
    extractVocabularyHelper (data, seqop)
  }

  def extractVocabularyDocFrequencyForTokenized (data: Dataset[Seq[String]]) = {
    val seqop : (Map[String, Int], Seq[String]) => Map[String, Int] =
      (a, xs) => xs.toSet.foldLeft (a) { (a, x) =>
        a + (x -> (a.getOrElse (x, 0) + 1))
      }
    extractVocabularyHelper (data, seqop)
  }

  /** Save vocabulary statistics
    * @param maybeFrequencySavePath: where to save raw term frequencies
    * @param maybeDocFrequencySavePath: where to save term doc frequencies
    *   (nb of documents into which a term may be found)
    * @param maybeIdfWeightSavePath: where to save idf weight for each term
    * @return (vocabularyFrequency, vocabularyDocFrequency) */
  def extractVocabulary (
    maybeFrequencySavePath: Option[String] = None, 
    maybeDocFrequencySavePath: Option[String] = None, 
    maybeIdfWeightSavePath: Option[String]
  ) = {
    def combop (a: scala.collection.immutable.Map[String, Int], b: scala.collection.immutable.Map[String, Int]) =
      b.foldLeft (a) { case (a, v) =>
        a + (v._1 -> (a.getOrElse (v._1, 0) + v._2))
      }

    val vocabularyFrequencyAbstracts = 
      extractVocabularyFrequencyForTokenized (pr (path_abstractsTokenized).
        map (_ getSeq[String](1)))
    val vocabularyFrequencyTitles = 
      extractVocabularyFrequencyForTokenized (pr (path_titlesTokenized).
        map (_ getSeq[String](1)))
    val vocabularyFrequency = combop (vocabularyFrequencyAbstracts, vocabularyFrequencyTitles)
    maybeFrequencySavePath foreach {
      sc.parallelize (vocabularyFrequency.toSeq).toDF ("token", "freq").
        write.mode ("overwrite").option ("path", _) save
    }
    
    val vocabularyDocFrequencyAbstracts = 
      extractVocabularyDocFrequencyForTokenized (pr (path_abstractsTokenized).
        map (_ getSeq[String](1)))
    val vocabularyDocFrequencyTitles = 
      extractVocabularyDocFrequencyForTokenized (pr (path_titlesTokenized).
        map (_ getSeq[String](1)))
    val vocabularyDocFrequency = combop (vocabularyDocFrequencyAbstracts, vocabularyFrequencyTitles)
    maybeDocFrequencySavePath foreach {
      sc.parallelize (vocabularyDocFrequency.toSeq).toDF ("token", "freq").
        write.mode ("overwrite").option ("path", _) save
    }

    maybeIdfWeightSavePath foreach {
      val nDocs = pr (path_abstractsTokenized) count
      val idfWeigths = vocabularyDocFrequency.
        map { case (w, n) => (w, Math.log ((1.0*nDocs) / n)) }.
        toSeq
      sc.parallelize (idfWeigths).toDF ("token", "weight").
        write.mode ("overwrite").option ("path", _) save
    }
    (vocabularyFrequency, vocabularyDocFrequency)
  }

  def extractWordEmbedding_1 (data: Dataset[Seq[String]], 
    embeddingSize: Int, 
    maybeSavePath: Option[String]
  ) : Word2VecModel = {
    var model: Word2VecModel = null
    try {
      data cache
      val w2v = new Word2Vec ()
      w2v.setVectorSize (embeddingSize)
      model = w2v fit data.rdd
    }
    finally {
      data unpersist
    }
    
    maybeSavePath foreach { path =>
      session.sparkContext.
        parallelize (model.getVectors.toList).toDF.
        write.mode("overwrite").option ("path", path) save
    }
    model
  }

  def extractWordEmbedding (
    data: Dataset[TokenizedRecord], 
    embeddingSize: Int, 
    maybeSavePath: Option[String] = None
  ) : Word2VecModel =
    extractWordEmbedding_1 (data.map {_ terms}, embeddingSize, maybeSavePath)

  /** Creates a weighted BoW from a tokenized dataset.
    * Keys in the input dataset may contain duplicates. */
  def createWeighted (
    data: Dataset[TokenizedRecord], 
    termsIdfPath: String = path_tokensIdfWeights, 
    maybeSavePath: Option[String] = None
  ) : Dataset[WeightedTermRecord] = {
    val termIdfWeights : Map[String, Double] = 
      pr (termsIdfPath).as[(String, Double)].collect.toMap
    val maxIdf = termIdfWeights.foldLeft (0.0) { (a, x) => Math.max (a, x._2) }
    val bTermIdfWeights = sc.broadcast (termIdfWeights)
    
    val ret = data.map { r => (r.id, r.terms) }.
      rdd.
      groupByKey.
      map { case (id, l) => (
        id,
        l.toSeq.flatMap { x => x }.
          foldLeft (Map.empty[String, Int]) { (a, t) => 
            a + (t -> (a.getOrElse (t, 0) + 1)) } )
      }.mapPartitions { it =>
        val termIdfWeights: Map[String, Double] = bTermIdfWeights.value        
        it map { case (id, localDic) =>
          val totalTermsInDoc: Double = 
            localDic.foldLeft (0) {(a, e) => a + e._2}
          WeightedTermRecord (
            id,
            localDic.foldLeft (Map.empty[String, Double]) { (a, e) =>
              val term = e._1
              val tf = e._2 / totalTermsInDoc
              val idf = termIdfWeights.getOrElse (term, maxIdf)
              a + (term -> tf*idf)
            } ) } }.
      toDF.as[WeightedTermRecord]

    maybeSavePath foreach { ret.write.mode ("overwrite").option ("path", _) save }
    ret
  }
 
  def computeManuscriptsEmbeddings (
    weightedBoWData: Dataset[WeightedTermRecord], 
    wordEmbeddingData: Dataset[(String, Seq[Float])], 
    maybeSavePath: Option[String] = None) = {
    val weightedBoWFlattenData = weightedBoWData.flatMap { r =>
      r.weights.toSeq.map { w =>
        val term: String = w._1
        val termWeight: Double = w._2
        val manuscriptId = r.id
        ( term, (manuscriptId, termWeight)) }
    }
    
    val weightsEmbeddingJoinData = wordEmbeddingData.rdd.join (weightedBoWFlattenData.rdd).map { x =>
      val termEmbedding: Seq[Float] = x._2._1
      val termWeight: Double = x._2._2._2
      val manuscriptId: String = x._2._2._1
      
      val weightedEmbedding = for (e <- termEmbedding) yield e*termWeight
      (manuscriptId, (termWeight, weightedEmbedding.toSeq))
    }
    
    val tfidf_embedding_agg_ds = weightsEmbeddingJoinData.reduceByKey { (a, b) =>
      (a._1 + b._1, (a._2 zip b._2) map { x => x._1 + x._2 })
    }
    
    val manuscriptsEmbedding = tfidf_embedding_agg_ds.mapValues { case (n, xs) => xs.map { _ / n } }.toDF ("id", "embedding")
    
    maybeSavePath foreach { manuscriptsEmbedding.write.option ("path", _) save }
    
    manuscriptsEmbedding
  }

  def computeTokensSeqEmbedding (
    tokens: Seq[String], 
    wordEmbeddingData: Dataset[WordEmbeddingRecord], 
    idfMap: Map[String, Double]
  ) : Array[Float] = {
    val tokenCount: Double = tokens size
    val tokenFrequencies = 
      tokens.foldLeft (tokens.map { (_, 0) }.toMap) { (a, t) => a + (t -> (a.getOrElse (t, 0) + 1)) }.toSeq
    val tokenWeights = tokenFrequencies.map { case (w, f) =>
      val tokenWeight: Double = idfMap.getOrElse (w, 0.0)
      (w, tokenWeight * (f / tokenCount))
    }
    val unnormalizedEmbedding =
      sc.parallelize (tokenWeights.toSeq).
        toDF ("word", "weight").
        join (wordEmbeddingData, "word").
        collect.
        map { r => (r getDouble 1, r getSeq[Float] 2) }.
        foldLeft ((0.0, Array.empty[Float])) { (a, x) =>
          (   a._1 + x._1,
            if (a._2.isEmpty) x._2.toArray
            else a._2.zip (x._2).map {x => x._1 + x._2 }
          ) }
    unnormalizedEmbedding._2.map { x =>  (x / unnormalizedEmbedding._1) toFloat }
  }

  def computeTextEmbedding (
    texts: Seq[String], 
    wordEmbeddingData: Dataset[WordEmbeddingRecord], 
    idfData: Dataset[(String, Double)]
  ) : Seq[Array[Float]] =
    computeTextEmbedding (texts, wordEmbeddingData, idfData.collect.toMap)
  
  def computeTextEmbedding (
    texts: Seq[String], 
    wordEmbeddingData: Dataset[WordEmbeddingRecord], 
    idfMap: Map[String, Double]
  ) : Seq[Array[Float]] =
    tokenizeTextPerSentence (texts) map { ts => computeTokensSeqEmbedding (ts, wordEmbeddingData, idfMap) }

  def createOneHotAreaEncodingForClassification (
    manuscriptsEmbeddingPath: String, 
    maybeSavePath: Option[String] = None, 
    maybeAreaIndexSavePath: Option[String] = None
  ) = {
    val manuscriptsEmbedding = pr (manuscriptsEmbeddingPath).toDF ("eid", "embedding")
    val areaToIndexMap : Map[String, Int] = flattenSubjAreas.
      select ("area").
      map { _ getString 0 }.
      distinct.
      collect.
      sorted.
      zipWithIndex.
      toMap

    val areaToIndexDS = sc.parallelize (areaToIndexMap.toSeq).toDF ("area", "areaIndex")
    val flattenSubjAreasIndex = flattenSubjAreas.join (areaToIndexDS, "area").select ("eid", "areaIndex")
    val OneHotEnc = flattenSubjAreasIndex.
      map { x => (x getString 0, x getInt 1) }.rdd.
      aggregateByKey (Array.fill[Int](27)(0)) (
        (a, i) => { a (i) = 1; a },
        (a, b) => (for (i <- 0 until a.size) yield (a (i) + b (i))).toArray).
      toDF("eid", "hotarea").
      join (manuscriptsEmbedding, "eid")
    
    maybeSavePath foreach { path => OneHotEnc.write.mode ("overwrite").option ("path", path) save }
    maybeAreaIndexSavePath foreach { path => areaToIndexDS.write.mode ("overwrite").option ("path", path) save }
    (OneHotEnc, areaToIndexDS)
  }

  def areaLRModel (
    areaIndex: Int, 
    OHEPath: String = path_areaOneHotEncodingForClassification, 
    trainProportion: Double=0.8
  ) = {
    val OneHotEnc = pr (OHEPath)
    val labeledEmbeddingMedi = OneHotEnc.select ("hotarea", "embedding").map { x =>
      val label = x.getSeq[Int](0)(areaIndex)
      val features = Vectors dense (x.getSeq[Double] (1).toArray)
      LabeledPoint (label, features)
    }
    val Array(training, testing) = labeledEmbeddingMedi.randomSplit (Array (trainProportion, 1.0 - trainProportion))
    val lr = new LogisticRegression ()
    val lrModel = lr.fit (training)
    (lrModel.transform (testing).filter {x => x.getDouble (0) == x.getDouble(4)}.count, testing count)
  }

  def allAreaOHELRModel (
    OHEPath: String = path_areaOneHotEncodingForClassification, 
    maybeAreaOHEIndexPath: Option[String], 
    trainProportion: Double=0.8, 
    maybePerformancesSavePath: Option[String]
  ) : Dataset[OHEClassificationModelPerformancesRecord] = {
    val modelPerformances = (0 until 27).foldLeft (List.empty[(Int, Long, Long, Double)]) { (a, i) =>
      val (fitCount, testCount) = areaLRModel (i, OHEPath)
      a :+ (i, fitCount, testCount, (100.0*fitCount) / testCount)
    }
    val modelPerformancesDF = sc.parallelize (modelPerformances).toDF ("areaIndex", "fitcount", "totalcount", "accuracy")
    val modelPerformancesWithLabelsDF =
      maybeAreaOHEIndexPath.
        map { path => pr (path).join (modelPerformancesDF, "areaIndex") }.
        getOrElse (modelPerformancesDF.withColumn ("label", $"areaIndex" cast StringType)).
        select ("label", "fitcount", "totalcount", "accuracy")
    maybePerformancesSavePath foreach { modelPerformancesWithLabelsDF.write.mode ("overwrite").option ("path", _) save }
    modelPerformancesWithLabelsDF.as[OHEClassificationModelPerformancesRecord]
  }

  def createISSN_OHEDataset (areas: Seq[String], maybeSavePath: Option[String] = None) = {
    val eids = flattenSubjAreas.where ($"area".isin (areas: _*)).select ("eid").distinct
    val issns = manuscripts.
      select ("eid", "issn").
      join (eids, "eid").
      groupBy ("issn").
      count.
      orderBy ($"count" desc).
      select ("issn").
      map {_ getString 0}.
      collect.
      zipWithIndex
    val issnDF = sc.parallelize (issns).toDF ("issn", "index")
    val N = issns.size
    val ret = manuscripts.
      select ("eid", "issn").
      join (issnDF, "issn").
      map { x =>
        val eid = x getString 1
        val index = x getInt 2
        val oheArray = Array.fill[Int](N)(0)
        oheArray (index) = 1
        (eid, oheArray)
      }.
      toDF ("id", "ohe")
    maybeSavePath foreach { ret.write.mode ("overwrite").option ("path", _) save }
    ret
  }

  def doitSaveTokenizedAbstracts =
    saveTokenizedAbstracts (manuscripts, "s3://wads/epfl/thy/manuscripts-content-tokenized-abstracts")

  def doitSaveTokenizePerSentence = {
    tokenizePerSentence (
      manuscripts.
        select ("eid", "abstr").
        map {x => (x getString 0, x getString 1)},
      None, Some (path_abstractsTokenized))

    tokenizePerSentence (
      manuscripts.
        select ("eid", "title").
        map {x => (x getString 0, x getString 1)},
      None, Some (path_titlesTokenized))
  }
  
  def doitExtractWordsEmbedding =
    extractWordEmbedding (session.read.parquet (path_abstractsTokenized).as[TokenizedRecord], 
      100, 
      Some (path_wordEmbeddingOnAbstracts))

  def doitExtractVocabulary =
    extractVocabulary (Some (path_tokensTotalFrequencies), Some (path_tokensDocFrequencies), Some (path_tokensIdfWeights))

  def doitSaveTFIDFWeighted =
    createWeighted (pr (path_abstractsTokenized).as[TokenizedRecord], maybeSavePath = Some (path_tfidfWeights))
 
  def doitExtractManuscriptsEmbedding = {
    val tfidf_ds = pr (path_tfidfWeights).as[WeightedTermRecord]
    val wordEmbedding_ds = pr (path_wordEmbeddingOnAbstracts).as[(String, Seq[Float])]
    computeManuscriptsEmbeddings (tfidf_ds, wordEmbedding_ds, Some(path_manuscriptsEmbedding))
  }

  def doitExtractAreaOHE4Classif =
    createOneHotAreaEncodingForClassification (
      path_manuscriptsEmbedding, 
      Some (path_areaOneHotEncodingForClassification), 
      Some (path_areaOneHotEncodingIndex))
  
  def doitAllAreaOHELRModels =
    allAreaOHELRModel (
      path_areaOneHotEncodingForClassification, 
      Some (path_areaOneHotEncodingIndex), 
      0.8, 
      Some (path_areaOneHotEncodingModelPerformances))
}

class ISSNClassifyOneAgainstOthers (areas: Seq[String], app: EmbeddingApp) (implicit session: SparkSession) {
  import EmbeddingApp._
  import session.implicits._

  val embedding = session.read.parquet (path_manuscriptsEmbedding)
  val eids = app.flattenSubjAreas.
    where ($"area" isin (areas:_*)).
    join (app.manuscripts, "eid").
    select ($"eid" as "id", $"issn")

  def classify (issns: Seq[String], trainProportion: Double = 0.8) = {
    val learn_eids = 
      app.manuscripts.
        where ($"issn" isin (issns:_*)).
        select ($"eid" as "id", $"issn")
    val complement = 
      eids.
        where (! ($"issn" isin (issns:_*))).
        join (embedding, "id").
        map { x =>
          val features = Vectors dense (x.getSeq[Double] (2).toArray)
          LabeledPoint (0.0, features)
        }
    val learn_data = 
      learn_eids.
        join (embedding, "id").
        map { x =>
          val issn = x getString 1
          val label = if (issn == issns(0)) 1.0 else 0.0
          val features = Vectors dense (x.getSeq[Double] (2).toArray)
          LabeledPoint (label, features)
        }
    val Array(training, testing) = learn_data.randomSplit (Array (trainProportion, 1.0 - trainProportion))
    val lr = new LogisticRegression ()
    val lrModel = lr.fit (training)
    (lrModel.transform (testing).filter {x => x.getDouble (0) == x.getDouble(4)}.count, testing count,
      lrModel.transform (complement).filter {x => x.getDouble (0) == x.getDouble(4)}.count, complement count)
  }

  def getIssnFreq (areas: Seq[String]) =
    app.manuscripts.
      select ("eid", "issn").
      join (app.flattenSubjAreas.where ($"area" isin (areas:_*)), "eid").
      groupBy ("issn").
      count.
      orderBy ($"count" desc)
}

object ISSNClassifyOneAgainstOthers {
    def demo (implicit session: SparkSession) = {
      val app = new EmbeddingApp
      val x = new ISSNClassifyOneAgainstOthers (List("MATE"), app)
      x.classify (List ("09258388","01694332", "19448244", "20507488"))
    }
}

