package com.stratio.governance.agent.searcher.model

import java.sql.{ResultSet, Timestamp}

import com.stratio.governance.agent.searcher.model.utils.TimestampUtils

object BusinessType extends Enumeration {
  type Value
  val TERM: BusinessType.Value = Value
  def fromString(value: String): BusinessType.Value = {
    BusinessType.withName(value)
  }
}

object BusinessStatus extends Enumeration {
  type Value
  val APR, PEN, UNR: BusinessStatus.Value = Value

  def fromString(value: String): BusinessStatus.Value = {
    BusinessStatus.withName(value)
  }
}

case class BusinessAsset( id: Int,
                          name: String,
                          description: String,
                          status: BusinessStatus.Value,
                          tpe: BusinessType.Value,
                          modifiedAt: Timestamp) extends EntityRow(id) {

  def this(id: Int, name: String, description: String, status: String, tpe: String, modifiedAt: String) =
    this(id, name, description, BusinessStatus.fromString(status), BusinessType.fromString(tpe), TimestampUtils.fromString(modifiedAt))
}

object BusinessAsset {

  def apply(id: Int, name: String, description: String, status: String, tpe: String, modifiedAt: String): BusinessAsset =
    new BusinessAsset(id, name, description, status, tpe, modifiedAt)

  @scala.annotation.tailrec
  def getValueFromResult(resultSet: ResultSet, list: List[BusinessAsset] = Nil): List[BusinessAsset] = {
    if (resultSet.next()) {
      val mod1: Timestamp = resultSet.getTimestamp(6)
      val mod2: Timestamp = resultSet.getTimestamp(7)
      val max = TimestampUtils.max(List(mod1, mod2))

      getValueFromResult(resultSet, BusinessAsset(resultSet.getInt(1),
                                                  resultSet.getString(2),
                                                  resultSet.getString(3),
                                                  BusinessStatus.fromString(resultSet.getString(4)),
                                                  BusinessType.fromString(resultSet.getString(5)),
                                                  max.get) :: list)
    } else {
      list
    }
  }
}
