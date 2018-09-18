package com.sodad.els.triage.config

/** tells where all objects must retrieved and saved */
trait PersistConfig {
  /** manuscripts corpora */
  def getPathManuscripts : String
  /** manuscript abstracts tokenized */
  def getPathAbstractsTokenized : String
  /** manuscript titles tokenized */
  def getPathTitlesTokenized : String
  /** absolute frequencies of tokens (terms) in the manuscript corpora */
  def getPathTokensTotalFrequencies : String
  /** for each term, the number of documents that contains it */
  def getPathTokensDocFrequencies : String
  /** weight (for example idf) of each term in the corpora */
  def getPathTokensIdfWeights : String
  /** the manuscripts as bag of words, weighted */
  def getPathTfidfWeights : String
  /** embeddings of the words of the corpora */
  def getPathWordEmbeddingOnAbstracts : String
  /** embeddings of the papers of the corpora */
  def getPathManuscriptsEmbedding : String
  /** embeddings of the journals */
  def getPathJournalsEmbedding : String
  /** embeddings of the journals, split per year */
  def getPathJournalsEmbeddingPerYear : String
  /** One Hot Encoding of areas, and manuscripts embeddings, 
    * usefull for area classifcation purpose */
  def getPathAreaOHE4Classification : String
  /** map the area label to its position in the ohe vector */
  def getPathAreaOHEIndex : String
  /** Performances of a area classification models */
  def getPathAreaOHEModelPerformances : String
  /** Reference distribution for trending dimension */
  def getPathTrendingReference : String
}

trait EmbeddingConfig {
  def dimension : Int
}

trait EmbeddingAppConfig {
  def embedding : EmbeddingConfig
  def persist : PersistConfig
}

class PrefixPersistConfig (val prefix: String) extends PersistConfig {
  def getPathManuscripts = 
    s"$prefix/manuscripts-content-valid-with-sources"
  def getPathAbstractsTokenized = 
    s"$prefix/manuscripts-abstracts-tokenized"
  def getPathTitlesTokenized = 
    s"$prefix/manuscripts-titles-tokenized"
  def getPathTokensTotalFrequencies = 
    s"$prefix/manuscripts-tokens-total-frequencies"
  def getPathTokensDocFrequencies = 
    s"$prefix/manuscripts-tokens-doc-frequencies"
  def getPathTokensIdfWeights = 
    s"$prefix/manuscripts-tokens-idf-weights"
  def getPathTfidfWeights = 
    s"$prefix/manuscripts-tfidf-weights"
  def getPathWordEmbeddingOnAbstracts = 
    s"$prefix/wordEmbeddingOnAbstracts"
  def getPathManuscriptsEmbedding = 
    s"$prefix/manuscriptsEmbedding"
  def getPathJournalsEmbedding =
    s"$prefix/journalsEmbedding"
  def getPathJournalsEmbeddingPerYear =
    s"$prefix/journalsEmbeddingPerYear"
  def getPathAreaOHE4Classification = 
    s"$prefix/areaOHE4classifData"
  def getPathAreaOHEIndex = s"$prefix/areaOHEIndex"
  def getPathAreaOHEModelPerformances = 
    s"$prefix/areaOHEModelPerformances"
  def getPathTrendingReference =
    s"$prefix/trendingReference"
}

object ElsStdEmbeddingConfig extends EmbeddingAppConfig {
  def embedding = new EmbeddingConfig {
    def dimension = 100
  }
  def persist = new PrefixPersistConfig ("s3://wads/epfl/thy")
}

