package com.stratio.governance.agent.search.testit

import com.stratio.governance.agent.searcher.main.AppConf
import org.scalatest.FlatSpec

class AppConfITTest extends FlatSpec {
  "DefaultConfigurationName" must "be testIT_config" in {
    assert (AppConf.configurationName=="testIT_config")
  }
}