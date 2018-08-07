package com.sodad.els.triage.config

object PlxStdEmbeddingConfig extends EmbeddingAppConfig {
  def embedding = new EmbeddingConfig {
    def dimension = 100
  }
  def persist = new PrefixPersistConfig ("phase6/std")
}
