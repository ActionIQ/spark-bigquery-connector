package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.spark.bigquery.pushdowns.TestConstants._
import org.scalatest.funsuite.AnyFunSuite

class SourceQuerySuite extends AnyFunSuite{

  private val sourceQuery = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME, Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS)
  private val sourceQuery2 = sourceQuery.copy(selectAttributes = true)

  test("sourceStatement") {
    assert(sourceQuery.sourceStatement.toString == "`test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS")
    assert(sourceQuery2.sourceStatement.toString == "`test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS")
  }

  test("suffixStatement") {
    assert(sourceQuery.suffixStatement.toString.isEmpty)
    assert(sourceQuery2.suffixStatement.toString.isEmpty)
  }

  test("columnSet") {
    assert(sourceQuery.columnSet.isEmpty)
    assert(sourceQuery2.columnSet.isEmpty)
  }

  test("processedProjections") {
    assert(sourceQuery.processedProjections.isEmpty)
    assert(sourceQuery2.processedProjections.isEmpty)
  }

  test("columns") {
    assert(sourceQuery.columns.isEmpty)
    assert(sourceQuery2.columns.exists(_.toString == "SCHOOLID , SCHOOLNAME"))
  }

  test("output") {
    assert(sourceQuery.output.size == 2)
    assert(sourceQuery.output == Seq(schoolIdAttributeReference, schoolNameAttributeReference))
    assert(sourceQuery2.output.size == 2)
    assert(sourceQuery2.output == Seq(schoolIdAttributeReference, schoolNameAttributeReference))
  }

  test("outputWithQualifier") {
    assert(sourceQuery.outputWithQualifier.size == 2)
    assert(sourceQuery.outputWithQualifier == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS))))
    assert(sourceQuery2.outputWithQualifier.size == 2)
    assert(sourceQuery2.outputWithQualifier == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS))))
  }

  test("nullableOutputWithQualifier") {
    val nullableOutputWithQualifier = sourceQuery.nullableOutputWithQualifier
    assert(nullableOutputWithQualifier.size == 2)
    assert(nullableOutputWithQualifier == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)).withNullability(true),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)).withNullability(true)))
    val nullableOutputWithQualifier2 = sourceQuery2.nullableOutputWithQualifier
    assert(nullableOutputWithQualifier2.size == 2)
    assert(nullableOutputWithQualifier2 == Seq(schoolIdAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)).withNullability(true),
      schoolNameAttributeReference.withQualifier(Seq(SUBQUERY_0_ALIAS)).withNullability(true)))
  }

  test("getStatement") {
    assert(sourceQuery.getStatement().toString == "SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS")
    assert(sourceQuery2.getStatement().toString == "SELECT SCHOOLID , SCHOOLNAME FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS")
  }

  test("getStatement with alias") {
    assert(sourceQuery.getStatement(useAlias = true).toString == "( SELECT * FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
    assert(sourceQuery2.getStatement(useAlias = true).toString == "( SELECT SCHOOLID , SCHOOLNAME FROM `test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS ) AS SUBQUERY_0")
  }

  test("getStatement with pushdownFilter set") {
    val sourceQueryWithPushdownFilter = SourceQuery(expressionConverter, expressionFactory, bigQueryRDDFactoryMock, TABLE_NAME,
      Seq(schoolIdAttributeReference, schoolNameAttributeReference), SUBQUERY_0_ALIAS, Option.apply("studentId = 1 AND studentName = Foo"))
    val sourceQueryWithPushdownFilter2 = sourceQueryWithPushdownFilter.copy(selectAttributes = true)
    assert(sourceQueryWithPushdownFilter.getStatement().toString == "SELECT * FROM " +
      "`test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS WHERE studentId = 1 AND studentName = Foo")
    assert(sourceQueryWithPushdownFilter2.getStatement().toString == "SELECT SCHOOLID , SCHOOLNAME FROM " +
      "`test_project:test_dataset.test_table` AS BQ_CONNECTOR_QUERY_ALIAS WHERE studentId = 1 AND studentName = Foo")
  }

  test("find") {
    val returnedQuery = sourceQuery.find({ case q: SourceQuery => q })
    assert(returnedQuery.exists(_ == sourceQuery))
    val returnedQuery2 = sourceQuery2.find({ case q: SourceQuery => q })
    assert(returnedQuery2.exists(_ == sourceQuery2))
  }
}
