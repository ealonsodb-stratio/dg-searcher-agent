package com.stratio.governance.agent.search.testit.utils.postgres.builder

abstract class AbstractBuilder[T] {
  def build: T
}
