package com.sodad.els.triage.config

class PlxPhase2PersistConfig 
    extends PrefixPersistConfig ("phase2")
{
  override def getPathTokensIdfWeights =
    s"phase6/std/manuscripts-tokens-idf-weights"
  override def getPathWordEmbeddingOnAbstracts = 
    s"phase6/std/wordEmbeddingOnAbstracts"
}

object PlxPhase2EmbeddingConfig extends EmbeddingAppConfig {
  def embedding = new EmbeddingConfig {
    def dimension = 100
  }
  def persist = new PlxPhase2PersistConfig
}

object PlxPhase2BuildEmbeddingConfig extends EmbeddingAppConfig {
  def embedding = new EmbeddingConfig {
    def dimension = 100
  }
  def persist = new PrefixPersistConfig ("phase2")
}
