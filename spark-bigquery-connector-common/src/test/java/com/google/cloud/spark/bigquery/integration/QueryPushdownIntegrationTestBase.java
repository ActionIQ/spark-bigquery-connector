/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;
import static java.time.temporal.TemporalAdjusters.firstDayOfYear;

import com.google.cloud.Tuple;
import com.google.cloud.spark.bigquery.BigQueryConnectorUtils;
import com.google.cloud.spark.bigquery.SparkBigQueryConfig.WriteMethod;
import com.google.cloud.spark.bigquery.integration.model.NumStruct;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.IsoFields;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Ignore;
import org.junit.Test;

public class QueryPushdownIntegrationTestBase extends SparkBigQueryIntegrationTestBase {

  private static <T> List<Tuple<Integer, T>> zipWithIndex(List<T> items) {
    AtomicInteger counter = new AtomicInteger();
    return items.stream()
        .map(element -> Tuple.of(counter.getAndIncrement(), element))
        .collect(Collectors.toList());
  }

  @Test
  public void testApproxCountDistinct() {
    Dataset<Row> df = readTestDataFromBigQuery("connector_dev", "connector_dev.dt");
    df.createOrReplaceTempView("dt");
    List<Row> results =
        spark
            .sql("select approx_count_distinct(id), approx_count_distinct(ts1) from dt")
            .collectAsList();
    assert (results.get(0).getLong(0) == 3);
    assert (results.get(0).getLong(1) == 1);
  }

  @Test
  public void testStringFunctionExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);
    df =
        df.selectExpr(
                "word",
                "ASCII(word) as ascii",
                "LENGTH(word) as length",
                "LOWER(word) as lower",
                "LPAD(word, 10, '*') as lpad",
                "RPAD(word, 10, '*') as rpad",
                "TRANSLATE(word, 'a', '*') as translate",
                "TRIM(concat('    ', word, '    ')) as trim",
                "LTRIM(concat('    ', word, '    ')) as ltrim",
                "RTRIM(concat('    ', word, '    ')) as rtrim",
                "UPPER(word) as upper",
                "INSTR(word, 'a') as instr",
                "INITCAP(word) as initcap",
                "CONCAT(word, '*', '!!') as concat",
                "FORMAT_STRING('*%s*', word) as format_string",
                "FORMAT_NUMBER(10.2345, 1) as format_number",
                "REGEXP_EXTRACT(word, '([A-Za-z]+$)', 1) as regexp_extract",
                "REGEXP_REPLACE(word, '([A-Za-z]+$)', 'replacement') as regexp_replace",
                "SUBSTR(word, 2, 2) as substr",
                "SOUNDEX(word) as soundex",
                "LIKE(word, '%aug%urs%') as like_with_percent",
                "LIKE(word, 'a_g_rs') as like_with_underscore",
                "LIKE(word, 'b_g_rs') as like_with_underscore_return_false",
                "FORMAT_NUMBER(CAST((word_count + 10000) AS FLOAT)/6, 3)",
                "FORMAT_NUMBER(word_count + 10000, 0)",
                "CONCAT_WS('-', word, word, word)")
            .where("word = 'augurs'");
    List<Row> result = df.collectAsList();
    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("augurs"); // word
    assertThat(r1.get(1)).isEqualTo(97); // ASCII(word)
    assertThat(r1.get(2)).isEqualTo(6); // LENGTH(word)
    assertThat(r1.get(3)).isEqualTo("augurs"); // LOWER(word)
    assertThat(r1.get(4)).isEqualTo("****augurs"); // LPAD(word, 10, '*')
    assertThat(r1.get(5)).isEqualTo("augurs****"); // LPAD(word, 10, '*')
    assertThat(r1.get(6)).isEqualTo("*ugurs"); // TRANSLATE(word, 'a', '*')
    assertThat(r1.get(7)).isEqualTo("augurs"); // TRIM(concat('    ', word, '    '))
    assertThat(r1.get(8)).isEqualTo("augurs    "); // LTRIM(concat('    ', word, '    '))
    assertThat(r1.get(9)).isEqualTo("    augurs"); // RTRIM(concat('    ', word, '    '))
    assertThat(r1.get(10)).isEqualTo("AUGURS"); // UPPER(word)
    assertThat(r1.get(11)).isEqualTo(1); // INSTR(word, 'a')
    assertThat(r1.get(12)).isEqualTo("Augurs"); // INITCAP(word)
    assertThat(r1.get(13)).isEqualTo("augurs*!!"); // CONCAT(word, '*', '!!')
    assertThat(r1.get(14)).isEqualTo("*augurs*"); // FORMAT_STRING('*%s*', word)
    assertThat(r1.get(15)).isEqualTo("10.2"); // FORMAT_NUMBER(10.2345, 1)
    assertThat(r1.get(16)).isEqualTo("augurs"); // REGEXP_EXTRACT(word, '([A-Za-z]+$)', 1)
    assertThat(r1.get(17))
        .isEqualTo("replacement"); // REGEXP_REPLACE(word, '([A-Za-z]+$)', 'replacement')
    assertThat(r1.get(18)).isEqualTo("ug"); // SUBSTR(word, 2, 2)
    assertThat(r1.get(19)).isEqualTo("A262"); // SOUNDEX(word)
    assertThat(r1.get(20)).isEqualTo(true); // LIKE(word, '%aug%urs%')
    assertThat(r1.get(21)).isEqualTo(true); // LIKE(word, 'a_g_rs')
    assertThat(r1.get(22)).isEqualTo(false); // LIKE(word, 'b_g_rs')
    assertThat(
        r1.getString(23)
            .equals("1,666.833")); // FORMAT_NUMBER(CAST((word_count + 10000) AS FLOAT)/6, 3)
    assertThat(r1.getString(24).equals("10,001")); // FORMAT_NUMBER(word_count + 10000, 0)
    assertThat(r1.getString(25).equals("augurs-augurs-augurs")); // CONCAT_WS('-', word, word, word)
  }

  @Test
  public void testDateFunctionExpressions() {
    // This table only has one row and one column which is today's date
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load("bigquery-public-data.google_political_ads.last_updated");

    df.createOrReplaceTempView("last_updated");

    List<Row> result =
        spark
            .sql(
                "SELECT "
                    + "report_data_updated_time, "
                    + "DATE_ADD(report_data_updated_time, 1), "
                    + "DATE_SUB(report_data_updated_time, 5), "
                    + "MONTH(report_data_updated_time), "
                    + "QUARTER(report_data_updated_time), "
                    + "YEAR(report_data_updated_time), "
                    + "TRUNC(report_data_updated_time, 'YEAR') "
                    + "FROM last_updated")
            .collectAsList();

    Row r1 = result.get(0);

    // Parsing the date rather than setting date to LocalDate.now() because the test will fail
    // in the edge case that the BigQuery read happens on an earlier date
    LocalDate date = LocalDateTime.parse(r1.get(0).toString()).toLocalDate();

    assertThat(r1.get(1).toString()).isEqualTo(date.plusDays(1L).toString()); // DATE_ADD
    assertThat(r1.get(2).toString()).isEqualTo(date.minusDays(5L).toString()); // DATE_SUB
    assertThat(r1.get(3)).isEqualTo(date.getMonth().getValue()); // MONTH
    assertThat(r1.get(4)).isEqualTo(date.get(IsoFields.QUARTER_OF_YEAR)); // QUARTER
    assertThat(r1.get(5)).isEqualTo(date.getYear()); // YEAR
    assertThat(r1.get(6).toString()).isEqualTo(date.with(firstDayOfYear()).toString()); // TRUNC
  }

  @Test
  public void testBasicExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);

    df.createOrReplaceTempView("shakespeare");

    List<Row> result =
        spark
            .sql(
                "SELECT "
                    + "word_count & corpus_date, "
                    + "word_count | corpus_date, "
                    + "word_count ^ corpus_date, "
                    + "~ word_count, "
                    + "word <=> corpus "
                    + "FROM shakespeare "
                    + "WHERE word = 'augurs' AND corpus = 'sonnets'")
            .collectAsList();

    // Note that for this row, word_count equals 1 and corpus_date equals 0
    Row r1 = result.get(0);
    assertThat(r1.get(0).toString()).isEqualTo("0"); // 1 & 0
    assertThat(r1.get(1).toString()).isEqualTo("1"); // 1 | 0
    assertThat(r1.get(2).toString()).isEqualTo("1"); // 1 ^ 0
    assertThat(r1.get(3).toString()).isEqualTo("-2"); // ~1
    assertThat(r1.get(4)).isEqualTo(false); // 'augurs' <=> 'sonnets'
  }

  @Test
  public void testMathematicalFunctionExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);
    df =
        df.selectExpr(
                "word",
                "word_count",
                "ABS(-22) as Abs",
                "ACOS(1) as Acos",
                "ASIN(0) as Asin",
                "ROUND(ATAN(0.5),2) as Atan",
                "COS(0) as Cos",
                "COSH(0) as Cosh",
                "ROUND(EXP(1),2) as Exp",
                "FLOOR(EXP(1)) as Floor",
                "GREATEST(1,5,3,4) as Greatest",
                "LEAST(1,5,3,4) as Least",
                "ROUND(LOG(word_count, 2.71), 2) as Log",
                "ROUND(LOG10(word_count), 2) as Log10",
                "POW(word_count, 2) as Pow",
                "ROUND(RAND(10),2) as Rand",
                "SIN(0) as Sin",
                "SINH(0) as Sinh",
                "ROUND(SQRT(word_count), 2) as sqrt",
                "TAN(0) as Tan",
                "TANH(0) as Tanh",
                "ISNAN(word_count) as IsNan",
                "SIGNUM(word_count) as Signum",
                "MD5(word)",
                "SHA1(word)",
                "SHA2(word, 256)",
                "CONV(SUBSTRING(CAST(MD5(word) AS STRING), 0, 5), 16, 10)",
                "word_count % 3")
            .where("word_count = 10 and word = 'glass'");
    List<Row> result = df.collectAsList();
    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("glass"); // word
    assertThat(r1.get(1)).isEqualTo(10); // word_count
    assertThat(r1.get(2)).isEqualTo(22); // ABS(-22)
    assertThat(r1.get(3)).isEqualTo(0.0); // ACOS(1)
    assertThat(r1.get(4)).isEqualTo(0.0); // ASIN(0)
    assertThat(r1.get(5)).isEqualTo(0.46); // ROUND(ATAN(0.5),2)
    assertThat(r1.get(6)).isEqualTo(1.0); // COS(0)
    assertThat(r1.get(7)).isEqualTo(1.0); // COSH(0)
    assertThat(r1.get(8)).isEqualTo(2.72); // ROUND(EXP(1),2)
    assertThat(r1.get(9)).isEqualTo(2); // FLOOR(EXP(1))
    assertThat(r1.get(10)).isEqualTo(5); // GREATEST(1,5,3,4)
    assertThat(r1.get(11)).isEqualTo(1); // LEAST(1,5,3,4)
    assertThat(r1.get(12)).isEqualTo(0.43); // ROUND(LOG(word_count, 2.71), 2)
    assertThat(r1.get(13)).isEqualTo(1.0); // ROUND(LOG10(word_count), 2)
    assertThat(r1.get(14)).isEqualTo(100.0); // POW(word_count, 2)
    assertThat(r1.get(16)).isEqualTo(0.0); // SIN(0)
    assertThat(r1.get(17)).isEqualTo(0.0); // SINH(0)
    assertThat(r1.get(18)).isEqualTo(3.16); // ROUND(SQRT(word_count), 2)
    assertThat(r1.get(19)).isEqualTo(0.0); // TAN(0)
    assertThat(r1.get(20)).isEqualTo(0.0); // TANH(0)
    assertThat(r1.get(21)).isEqualTo(false); // ISNAN(word_count)
    assertThat(r1.get(22)).isEqualTo(1.0); // SIGNUM(word_count)
    assertThat(r1.getString(23) == "JXDJGfXvHXCR8PZtVNrJdA=="); // MD5(word)
    assertThat(r1.getString(24) == "Fw/X6EobL5cjcwEC0OkFF6kXWII="); // SHA1(word)
    assertThat(
        r1.getString(25) == "EyoaORzOGBxJCixDUFlyIfrZriB3FNH5oQ9nvLHIKI0="); // SHA2(word, 256)
    assertThat(r1.getString(26) == "153356");
    assertThat(r1.getLong(27) == 1L);
  }

  @Test
  public void testMiscellaneousExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);
    df.createOrReplaceTempView("shakespeare");
    df =
        df.selectExpr(
                "word",
                "word_count AS WordCount",
                "CAST(word_count as string) AS cast",
                "SHIFTLEFT(word_count, 1) AS ShiftLeft",
                "SHIFTRIGHT(word_count, 1) AS ShiftRight",
                "CASE WHEN word_count > 10 THEN 'frequent' WHEN word_count <= 10 AND word_count > 4 THEN 'normal' ELSE 'rare' END AS WordFrequency",
                "(SELECT MAX(word_count) from shakespeare) as MaxWordCount",
                "(SELECT MAX(word_count) from shakespeare WHERE word IN ('glass', 'augurs')) as MaxWordCountInWords",
                "COALESCE(NULL, NULL, NULL, word, NULL, 'Push', 'Down') as Coalesce",
                "IF(word_count = 10 and word = 'glass', 'working', 'not working') AS IfCondition",
                "-(word_count) AS UnaryMinus",
                "CAST(word_count + 1.99 as DECIMAL(17, 2)) / CAST(word_count + 2.99 as DECIMAL(17, 1)) < 0.9")
            .where("word_count = 10 and word = 'glass'")
            .orderBy("word_count");

    List<Row> result = df.collectAsList();
    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("glass"); // word
    assertThat(r1.get(1)).isEqualTo(10); // word_count
    assertThat(r1.get(2)).isEqualTo("10"); // word_count
    assertThat(r1.get(3)).isEqualTo(20); // SHIFTLEFT(word_count, 1)
    assertThat(r1.get(4)).isEqualTo(5); // SHIFTRIGHT(word_count, 1)
    assertThat(r1.get(5)).isEqualTo("normal"); // CASE WHEN
    assertThat(r1.get(6)).isEqualTo(995); // SCALAR SUBQUERY
    assertThat(r1.get(7)).isEqualTo(10); // SCALAR SUBQUERY WITH IN
    assertThat(r1.get(8)).isEqualTo("glass"); // COALESCE
    assertThat(r1.get(9)).isEqualTo("working"); // IF CONDITION
    assertThat(r1.get(10)).isEqualTo(-10); // UNARY MINUS
    assertThat(r1.get(11)).isEqualTo(false); // CHECKOVERFLOW
  }

  @Test
  public void testUnionQuery() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);

    df.createOrReplaceTempView("shakespeare");
    Dataset<Row> words_with_word_count_100 =
        spark.sql("SELECT word, word_count FROM shakespeare WHERE word_count = 100");
    Dataset<Row> words_with_word_count_150 =
        spark.sql("SELECT word, word_count FROM shakespeare WHERE word_count = 150");

    List<Row> unionList =
        words_with_word_count_100.union(words_with_word_count_150).collectAsList();
    List<Row> unionAllList =
        words_with_word_count_150.unionAll(words_with_word_count_100).collectAsList();
    List<Row> unionByNameList =
        words_with_word_count_100.unionByName(words_with_word_count_150).collectAsList();
    assertThat(unionList.size()).isGreaterThan(0);
    assertThat(unionList.get(0).get(1)).isAnyOf(100L, 150L);
    assertThat(unionAllList.size()).isGreaterThan(0);
    assertThat(unionAllList.get(0).get(1)).isAnyOf(100L, 150L);
    assertThat(unionByNameList.size()).isGreaterThan(0);
    assertThat(unionByNameList.get(0).get(1)).isAnyOf(100L, 150L);
  }

  @Test
  public void testBooleanExpressions() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);

    df.createOrReplaceTempView("shakespeare");

    List<Row> result =
        spark
            .sql(
                "SELECT "
                    + "word, "
                    + "word LIKE '%las%' AS Contains, "
                    + "word LIKE '%lass' AS Ends_With, "
                    + "word LIKE 'gla%' AS Starts_With "
                    + "FROM shakespeare "
                    + "WHERE word IN ('glass', 'very_random_word') AND word_count != 99")
            .collectAsList();

    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo("glass"); // word
    assertThat(r1.get(1)).isEqualTo(true); // contains
    assertThat(r1.get(2)).isEqualTo(true); // ends_With
    assertThat(r1.get(3)).isEqualTo(true); // starts_With

    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    df = readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);
    df.createOrReplaceTempView("numStructDF");

    result =
        spark
            .sql(
                "SELECT "
                    + "num1 == num2 AS EqualTo, "
                    + "num1 > num2 AS GreaterThan, "
                    + "num1 < num2 AS LessThan, "
                    + "num1 >= num2 AS GreaterThanEqualTo, "
                    + "num1 <= num2 AS LessThanEqualTo, "
                    + "num1 != num2 AS NotEqualTo, "
                    + "ISNULL(num1) AS IsNull, "
                    + "ISNOTNULL(num2) AS IsNotNull, "
                    + "num3 IN (1,2) AS In "
                    + "FROM numStructDF")
            .collectAsList();
    r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo(false); // EqualTo
    assertThat(r1.get(1)).isEqualTo(true); // GreaterThan
    assertThat(r1.get(2)).isEqualTo(false); // LessThan
    assertThat(r1.get(3)).isEqualTo(true); // GreaterThanEqualTo
    assertThat(r1.get(4)).isEqualTo(false); // LessThanEqualTo
    assertThat(r1.get(5)).isEqualTo(true); // NotEqualTo
    assertThat(r1.get(6)).isEqualTo(false); // IsNull
    assertThat(r1.get(7)).isEqualTo(true); // IsNotNull
    assertThat(r1.get(8)).isEqualTo(true); // In
  }

  @Ignore("TODO: give this more memory")
  public void testWindowStatements() {
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(TestConstants.SHAKESPEARE_TABLE);

    df.createOrReplaceTempView("shakespeare");

    df =
        spark.sql(
            "SELECT "
                + "*, "
                + "ROW_NUMBER() OVER (PARTITION BY corpus ORDER BY corpus_date) as row_number, "
                + "RANK() OVER (PARTITION BY corpus ORDER BY corpus_date) as rank, "
                + "DENSE_RANK() OVER (PARTITION BY corpus ORDER BY corpus_date) as dense_rank, "
                + "PERCENT_RANK() OVER (PARTITION BY corpus ORDER BY corpus_date) as percent_rank, "
                + "AVG(word_count) OVER (PARTITION BY corpus) as word_count_avg_by_corpus, "
                + "COUNT(word) OVER (PARTITION BY corpus ORDER BY corpus_date) as num_of_words_in_corpus, "
                + "COUNT(word) OVER count_window as num_of_words_in_corpus_window_clause "
                + "FROM shakespeare "
                + "WINDOW count_window AS (PARTITION BY corpus ORDER BY corpus_date)");
    /**
     * The reason I am filtering the dataframe later instead of adding where clause to the sql query
     * is, in SQL the window statement would be executed after the where clause filtering is done.
     * In order to test the appropriate behaviour, added the filtering port later.
     */
    Object[] filteredRow =
        df.collectAsList().stream()
            .filter(row -> row.get(0).equals("augurs") && row.get(2).equals("sonnets"))
            .toArray();
    assertThat(filteredRow.length).isEqualTo(1);
    GenericRowWithSchema row = (GenericRowWithSchema) filteredRow[0];
    assertThat(row.get(4))
        .isEqualTo(2); // ROW_NUMBER() OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(5)).isEqualTo(1); // RANK() OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(6))
        .isEqualTo(1); // DENSE_RANK() OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(7))
        .isEqualTo(0.0); // PERCENT_RANK() OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(8))
        .isEqualTo(4.842262714169159); // AVG(word_count) OVER (PARTITION BY corpus)
    assertThat(row.get(9))
        .isEqualTo(3677); // COUNT(word) OVER (PARTITION BY corpus ORDER BY corpus_date)
    assertThat(row.get(10)).isEqualTo(3677); // COUNT(word) OVER count_window
  }

  @Test
  public void testWindowQueryWithWindowSpec() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    // Appending more data to the existing table to perform WindowOperation
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDatasetForWindow),
        testDataset.toString() + "." + testTable);
    Dataset<Row> df =
        spark
            .read()
            .format("bigquery")
            .option("materializationDataset", testDataset.toString())
            .load(testDataset.toString() + "." + testTable);
    df.createOrReplaceTempView("numStructDF");
    /*
     +----+----+----+--------------------+
     |num1|num2|num3|             strings|
     +----+----+----+--------------------+
     |   3|   2|   1|[[2:str1, 3:str2,...|
     |   4|   3|   2|[[3:str1, 4:str2,...|
     |   4|   3|   1|[[2:str1, 3:str2,...|
     |   6|   2|   2|[[3:str1, 4:str2,...|
     |   5|   3|   1|[[2:str1, 3:str2,...|
     |   7|   2|   2|[[3:str1, 4:str2,...|
     +----+----+----+--------------------+
    */

    List<Row> result =
        spark
            .sql(
                "SELECT "
                    + "DISTINCT num3, "
                    + "num2, "
                    + "SUM(num1) OVER (PARTITION BY num3,num2 ORDER BY num2 ASC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sum_num1 "
                    + "FROM numStructDF where num3 = 2 and num2 = 3")
            .collectAsList();
    /*
     +----+----+--------+
     |num3|num2|sum_num1|
     +----+----+--------+
     |   2|   3|       4|
     +----+----+--------+
    */
    assertThat(result.size()).isEqualTo(1);
    Row r = result.get(0);
    assertThat(r.get(0)).isEqualTo(2);
    assertThat(r.get(1)).isEqualTo(3);
    assertThat(r.get(2)).isEqualTo(4);
  }

  @Test
  public void testAggregateExpressions() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    Dataset<Row> df =
        readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);
    df.createOrReplaceTempView("numStructDF");

    List<Row> result =
        spark
            .sql(
                "SELECT "
                    + "AVG(num1) as average, "
                    + "CORR(num1, num2) as corr, "
                    + "COVAR_POP(num1, num2) as covar_pop, "
                    + "COVAR_SAMP(num1, num2) as covar_samp, "
                    + "COUNT(*) as count, "
                    + "MAX(num1) as max, "
                    + "MIN(num1) as min, "
                    + "SUM(num1) as sum, "
                    + "STDDEV_POP(num1) as stddev_pop, "
                    + "ROUND(STDDEV_SAMP(num1),2) as stddev_samp, "
                    + "VAR_POP(num1) as var_pop, "
                    + "VAR_SAMP(num1) as var_samp "
                    + "FROM numStructDF ")
            .collectAsList();

    Row r1 = result.get(0);
    assertThat(r1.get(0)).isEqualTo(3.5); // AVG(num1)
    assertThat(r1.get(1)).isEqualTo(1.0); // CORR(num1, num2)
    assertThat(r1.get(2)).isEqualTo(0.25); // COVAR_POP(num1, num2)
    assertThat(r1.get(3)).isEqualTo(0.5); // COVAR_SAMP(num1, num2)
    assertThat(r1.get(4)).isEqualTo(2); // COUNT(*)
    assertThat(r1.get(5)).isEqualTo(4); // MAX(num1)
    assertThat(r1.get(6)).isEqualTo(3); // MIN(num1)
    assertThat(r1.get(7)).isEqualTo(7); // SUM(num1)
    assertThat(r1.get(8)).isEqualTo(0.5); // STDDEV_POP(num1)
    assertThat(r1.get(9)).isEqualTo(0.71); // ROUND(STDDEV_SAMP(num1),2)
    assertThat(r1.get(10)).isEqualTo(0.25); // VAR_POP(num1)
    assertThat(r1.get(11)).isEqualTo(0.5); // VAR_SAMP(num1)
  }

  @Test
  public void testInnerJoin() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    // Creating a DataFrame of schema NumStruct, and writing it to BigQuery
    Dataset<Row> df =
        readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);

    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDatasetForJoin),
        testDataset.toString() + "." + testTable + "_to_join");
    // Creating an additional DataFrame of schema NumStruct, and writing it to BigQuery which will
    // be used for join
    Dataset<Row> df_to_join =
        readTestDataFromBigQuery(
            testDataset.toString(), testDataset.toString() + "." + testTable + "_to_join");

    // disabling pushdown to collect the join result to compare with pushdown enabled
    BigQueryConnectorUtils.disablePushdownSession(spark);
    List<Row> withoutPushDownResult =
        df.join(df_to_join, df.col("num1").equalTo(df_to_join.col("num1"))).collectAsList();

    // enabling pushdown to test join
    BigQueryConnectorUtils.enablePushdownSession(spark);
    List<Row> withPushDownResult =
        df.join(df_to_join, df.col("num1").equalTo(df_to_join.col("num1"))).collectAsList();
    // checking if the results with and without pushdown is the same
    assertThat(withPushDownResult.size()).isEqualTo(withoutPushDownResult.size());
    assertThat(withoutPushDownResult.containsAll(withPushDownResult)).isTrue();
    assertThat(withPushDownResult.containsAll(withoutPushDownResult)).isTrue();
    /*
     +----+----+----+--------------------+----+----+----+--------------------+
     |num1|num2|num3|             strings|num1|num2|num3|             strings|
     +----+----+----+--------------------+----+----+----+--------------------+
     |   4|   3|   2|[[[[3:str1, 4:str...|   4|   1|   3|[[[[1:str1, 4:str...|
     |   3|   2|   1|[[[[2:str1, 3:str...|   3|   4|   3|[[[[4:str4, 3:str...|
     +----+----+----+--------------------+----+----+----+--------------------+
    */
    assertThat(withPushDownResult.size()).isEqualTo(2);
    for (Row r : withPushDownResult) {
      assertThat(r.get(0)).isEqualTo(r.get(4));
    }

    // swapping the tables
    List<Row> result =
        df_to_join.join(df, df.col("num1").equalTo(df_to_join.col("num1"))).collectAsList();
    /*
     +----+----+----+--------------------+----+----+----+--------------------+
     |num1|num2|num3|             strings|num1|num2|num3|             strings|
     +----+----+----+--------------------+----+----+----+--------------------+
     |   4|   1|   3|[[[[1:str1, 4:str...|   4|   3|   2|[[[[3:str1, 4:str...|
     |   3|   4|   3|[[[[4:str4, 3:str...|   3|   2|   1|[[[[2:str1, 3:str...|
     +----+----+----+--------------------+----+----+----+--------------------+
    */

    assertThat(result.size()).isEqualTo(2);
    for (Row r : result) {
      assertThat(r.size()).isEqualTo(8);
      assertThat(r.get(0)).isEqualTo(r.get(4));
    }
  }

  @Test
  public void testLeftOuterJoin() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    Dataset<Row> df =
        readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);

    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDatasetForJoin),
        testDataset.toString() + "." + testTable + "_to_join");
    Dataset<Row> df_to_join =
        readTestDataFromBigQuery(
            testDataset.toString(), testDataset.toString() + "." + testTable + "_to_join");

    // disabling pushdown to collect the join result to compare with pushdown enabled
    BigQueryConnectorUtils.disablePushdownSession(spark);
    Dataset<Row> dfWithoutPushDownResult =
        df.alias("num_struct")
            .join(
                df_to_join.alias("num_struct_to_join"),
                df.col("num1").equalTo(df_to_join.col("num1")),
                "leftouter")
            .select(
                "num_struct.num1",
                "num_struct.num2",
                "num_struct.num3",
                "num_struct_to_join.num1",
                "num_struct_to_join.num2",
                "num_struct_to_join.num3");
    List<Row> withoutPushDownResult = dfWithoutPushDownResult.collectAsList();

    // enabling pushdown to test join
    BigQueryConnectorUtils.enablePushdownSession(spark);
    Dataset<Row> dfWithPushDownResult =
        df.alias("num_struct")
            .join(
                df_to_join.alias("num_struct_to_join"),
                df.col("num1").equalTo(df_to_join.col("num1")),
                "leftouter")
            .select(
                "num_struct.num1",
                "num_struct.num2",
                "num_struct.num3",
                "num_struct_to_join.num1",
                "num_struct_to_join.num2",
                "num_struct_to_join.num3");
    List<Row> withPushDownResult = dfWithPushDownResult.collectAsList();

    // checking if the results with and without pushdown is the same
    assertThat(withPushDownResult.size()).isEqualTo(withoutPushDownResult.size());
    assertThat(withoutPushDownResult.containsAll(withPushDownResult)).isTrue();
    assertThat(withPushDownResult.containsAll(withoutPushDownResult)).isTrue();
    /*
      +----+----+----+----+----+----+
      |num1|num2|num3|num1|num2|num3|
      +----+----+----+----+----+----+
      |   4|   3|   2|   4|   1|   3|
      |   3|   2|   1|   3|   4|   1|
      +----+----+----+----+----+----+
    */
    assertThat(withPushDownResult.size()).isEqualTo(2);
    for (Row r : withPushDownResult) {
      assertThat(r.size()).isEqualTo(6);
      assertThat(r.get(0)).isEqualTo(r.get(3));
    }

    // swapping the tables
    List<Row> result =
        df_to_join
            .alias("num_struct_to_join")
            .join(
                df.alias("num_struct"), df.col("num1").equalTo(df_to_join.col("num1")), "leftouter")
            .select(
                "num_struct_to_join.num1",
                "num_struct_to_join.num2",
                "num_struct_to_join.num3",
                "num_struct.num1",
                "num_struct.num2",
                "num_struct.num3")
            .collectAsList();
    /*
      +----+----+----+----+----+----+
      |num1|num2|num3|num1|num2|num3|
      +----+----+----+----+----+----+
      |   3|   4|   1|   3|   2|   1|
      |   4|   1|   3|   4|   3|   2|
      |   6|   5|   3|null|null|null|
      +----+----+----+----+----+----+
    */
    assertThat(result.size()).isEqualTo(3);
    for (Row r : result) {
      assertThat(r.size()).isEqualTo(6);
      if (r.get(3) == null) {
        assertThat(r.get(0)).isEqualTo(6);
      } else {
        assertThat(r.get(0)).isEqualTo(r.get(3));
      }
    }
  }

  @Test
  public void testRightOuterJoin() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    Dataset<Row> df =
        readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);

    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDatasetForJoin),
        testDataset.toString() + "." + testTable + "_to_join");
    Dataset<Row> df_to_join =
        readTestDataFromBigQuery(
            testDataset.toString(), testDataset.toString() + "." + testTable + "_to_join");

    // disabling pushdown to collect the join result to compare with pushdown enabled
    BigQueryConnectorUtils.disablePushdownSession(spark);
    Dataset<Row> dfWithoutPushDownResult =
        df.alias("num_struct")
            .join(
                df_to_join.alias("num_struct_to_join"),
                df.col("num1").equalTo(df_to_join.col("num1")),
                "rightouter")
            .select(
                "num_struct.num1",
                "num_struct.num2",
                "num_struct.num3",
                "num_struct_to_join.num1",
                "num_struct_to_join.num2",
                "num_struct_to_join.num3");
    List<Row> withoutPushDownResult = dfWithoutPushDownResult.collectAsList();

    // enabling pushdown to test join
    BigQueryConnectorUtils.enablePushdownSession(spark);
    Dataset<Row> dfWithPushDownResult =
        df.alias("num_struct")
            .join(
                df_to_join.alias("num_struct_to_join"),
                df.col("num1").equalTo(df_to_join.col("num1")),
                "rightouter")
            .select(
                "num_struct.num1",
                "num_struct.num2",
                "num_struct.num3",
                "num_struct_to_join.num1",
                "num_struct_to_join.num2",
                "num_struct_to_join.num3");
    List<Row> withPushDownResult = dfWithPushDownResult.collectAsList();

    // checking if the results with and without pushdown is the same
    assertThat(withPushDownResult.size()).isEqualTo(withoutPushDownResult.size());
    assertThat(withoutPushDownResult.containsAll(withPushDownResult)).isTrue();
    assertThat(withPushDownResult.containsAll(withoutPushDownResult)).isTrue();
    /*
      +----+----+----+----+----+----+
      |num1|num2|num3|num1|num2|num3|
      +----+----+----+----+----+----+
      |   3|   2|   1|   3|   4|   1|
      |null|null|null|   6|   5|   3|
      |   4|   3|   2|   4|   1|   3|
      +----+----+----+----+----+----+
    */
    assertThat(withPushDownResult.size()).isEqualTo(3);
    for (Row r : withPushDownResult) {
      assertThat(r.size()).isEqualTo(6);
      if (r.get(0) == null) {
        assertThat(r.get(3)).isEqualTo(6);
      } else {
        assertThat(r.get(0)).isEqualTo(r.get(3));
      }
    }

    // swapping the tables
    List<Row> result =
        df_to_join
            .alias("num_struct_to_join")
            .join(
                df.alias("num_struct"),
                df.col("num1").equalTo(df_to_join.col("num1")),
                "rightouter")
            .select(
                "num_struct_to_join.num1",
                "num_struct_to_join.num2",
                "num_struct_to_join.num3",
                "num_struct.num1",
                "num_struct.num2",
                "num_struct.num3")
            .collectAsList();
    /*
      +----+----+----+----+----+----+
      |num1|num2|num3|num1|num2|num3|
      +----+----+----+----+----+----+
      |   3|   4|   1|   3|   2|   1|
      |   4|   1|   3|   4|   3|   2|
      +----+----+----+----+----+----+
    */
    assertThat(result.size()).isEqualTo(2);
    for (Row r : result) {
      assertThat(r.size()).isEqualTo(6);
      assertThat(r.get(0)).isEqualTo(r.get(3));
    }
  }

  @Test
  public void testFullOuterJoin() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    Dataset<Row> df =
        readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);

    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDatasetForJoin),
        testDataset.toString() + "." + testTable + "_to_join");
    Dataset<Row> df_to_join =
        readTestDataFromBigQuery(
            testDataset.toString(), testDataset.toString() + "." + testTable + "_to_join");

    // disabling pushdown to collect the join result to compare with pushdown enabled
    BigQueryConnectorUtils.disablePushdownSession(spark);
    Dataset<Row> dfWithoutPushDownResult =
        df.alias("num_struct")
            .join(
                df_to_join.alias("num_struct_to_join"),
                df.col("num1").equalTo(df_to_join.col("num1")),
                "fullouter")
            .select(
                "num_struct.num1",
                "num_struct.num2",
                "num_struct.num3",
                "num_struct_to_join.num1",
                "num_struct_to_join.num2",
                "num_struct_to_join.num3");
    List<Row> withoutPushDownResult = dfWithoutPushDownResult.collectAsList();

    // enabling pushdown to test join
    BigQueryConnectorUtils.enablePushdownSession(spark);
    Dataset<Row> dfWithPushDownResult =
        df.alias("num_struct")
            .join(
                df_to_join.alias("num_struct_to_join"),
                df.col("num1").equalTo(df_to_join.col("num1")),
                "fullouter")
            .select(
                "num_struct.num1",
                "num_struct.num2",
                "num_struct.num3",
                "num_struct_to_join.num1",
                "num_struct_to_join.num2",
                "num_struct_to_join.num3");
    List<Row> withPushDownResult = dfWithPushDownResult.collectAsList();
    // checking if the results with and without pushdown is the same
    assertThat(withPushDownResult.size()).isEqualTo(withoutPushDownResult.size());
    assertThat(withoutPushDownResult.containsAll(withPushDownResult)).isTrue();
    assertThat(withPushDownResult.containsAll(withoutPushDownResult)).isTrue();
    /*
      +----+----+----+----+----+----+
      |num1|num2|num3|num1|num2|num3|
      +----+----+----+----+----+----+
      |   3|   2|   1|   3|   4|   1|
      |   4|   3|   2|   4|   1|   3|
      |null|null|null|   6|   5|   3|
      +----+----+----+----+----+----+
    */
    assertThat(withPushDownResult.size()).isEqualTo(3);
    for (Row r : withPushDownResult) {
      assertThat(r.size()).isEqualTo(6);
      if (r.get(0) == null) {
        assertThat(r.get(3)).isEqualTo(6);
      } else {
        assertThat(r.get(0)).isEqualTo(r.get(3));
      }
    }

    // swapping the tables
    List<Row> result =
        df_to_join
            .alias("num_struct_to_join")
            .join(
                df.alias("num_struct"), df.col("num1").equalTo(df_to_join.col("num1")), "fullouter")
            .select(
                "num_struct_to_join.num1",
                "num_struct_to_join.num2",
                "num_struct_to_join.num3",
                "num_struct.num1",
                "num_struct.num2",
                "num_struct.num3")
            .collectAsList();
    /*
      +----+----+----+----+----+----+
      |num1|num2|num3|num1|num2|num3|
      +----+----+----+----+----+----+
      |   4|   1|   3|   4|   3|   2|
      |   6|   5|   3|null|null|null|
      |   3|   4|   1|   3|   2|   1|
      +----+----+----+----+----+----+
    */
    assertThat(result.size()).isEqualTo(3);
    for (Row r : result) {
      assertThat(r.size()).isEqualTo(6);
      if (r.get(3) == null) {
        assertThat(r.get(0)).isEqualTo(6);
      } else {
        assertThat(r.get(0)).isEqualTo(r.get(3));
      }
    }
  }

  @Test
  public void testCrossJoin() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    Dataset<Row> df =
        readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);

    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDatasetForJoin),
        testDataset.toString() + "." + testTable + "_to_join");
    Dataset<Row> df_to_join =
        readTestDataFromBigQuery(
            testDataset.toString(), testDataset.toString() + "." + testTable + "_to_join");

    // disabling pushdown to collect the join result to compare with pushdown enabled
    BigQueryConnectorUtils.disablePushdownSession(spark);
    List<Row> withoutPushDownResult = df.crossJoin(df_to_join).collectAsList();

    // enabling pushdown to test join
    BigQueryConnectorUtils.enablePushdownSession(spark);
    List<Row> withPushDownResult = df.crossJoin(df_to_join).collectAsList();

    // checking if the results with and without pushdown is the same
    assertThat(withPushDownResult.size()).isEqualTo(withoutPushDownResult.size());
    assertThat(withoutPushDownResult.containsAll(withPushDownResult)).isTrue();
    assertThat(withPushDownResult.containsAll(withoutPushDownResult)).isTrue();
    /*
     +----+----+----+--------------------+----+----+----+--------------------+
     |num1|num2|num3|             strings|num1|num2|num3|             strings|
     +----+----+----+--------------------+----+----+----+--------------------+
     |   3|   2|   1|[[[[2:str1, 3:str...|   4|   1|   3|[[[[1:str1, 4:str...|
     |   4|   3|   2|[[[[3:str1, 4:str...|   4|   1|   3|[[[[1:str1, 4:str...|
     |   4|   3|   2|[[[[3:str1, 4:str...|   3|   4|   3|[[[[4:str4, 3:str...|
     |   3|   2|   1|[[[[2:str1, 3:str...|   3|   4|   3|[[[[4:str4, 3:str...|
     |   3|   2|   1|[[[[2:str1, 3:str...|   6|   5|   3|[[[[5:str5, 3:str...|
     |   4|   3|   2|[[[[3:str1, 4:str...|   6|   5|   3|[[[[5:str5, 3:str...|
     +----+----+----+--------------------+----+----+----+--------------------+
    */
    // swapping the tables
    assertThat(df_to_join.crossJoin(df).collectAsList().size()).isEqualTo(6);
  }

  @Test
  public void testLeftSemiJoin() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    Dataset<Row> df =
        readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);

    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDatasetForJoin),
        testDataset.toString() + "." + testTable + "_to_join");
    Dataset<Row> df_to_join =
        readTestDataFromBigQuery(
            testDataset.toString(), testDataset.toString() + "." + testTable + "_to_join");

    // disabling pushdown to collect the join result to compare with pushdown enabled
    BigQueryConnectorUtils.disablePushdownSession(spark);
    List<Row> withoutPushDownResult =
        df.join(df_to_join, df.col("num1").equalTo(df_to_join.col("num1")), "leftsemi")
            .collectAsList();

    // enabling pushdown to test join
    BigQueryConnectorUtils.enablePushdownSession(spark);
    List<Row> withPushDownResult =
        df.join(df_to_join, df.col("num1").equalTo(df_to_join.col("num1")), "leftsemi")
            .collectAsList();

    // checking if the results with and without pushdown is the same
    assertThat(withPushDownResult.size()).isEqualTo(withoutPushDownResult.size());
    assertThat(withoutPushDownResult.containsAll(withPushDownResult)).isTrue();
    assertThat(withPushDownResult.containsAll(withoutPushDownResult)).isTrue();
    /*
     +----+----+----+--------------------+
     |num1|num2|num3|             strings|
     +----+----+----+--------------------+
     |   4|   3|   2|[[[[3:str1, 4:str...|
     |   3|   2|   1|[[[[2:str1, 3:str...|
     +----+----+----+--------------------+
    */
    assertThat(withPushDownResult.size()).isEqualTo(2);
    for (Row r : withPushDownResult) {
      assertThat(r.size()).isEqualTo(4);
    }

    // swapping the tables
    List<Row> result =
        df_to_join
            .join(df, df.col("num1").equalTo(df_to_join.col("num1")), "leftsemi")
            .collectAsList();
    /*
     +----+----+----+--------------------+
     |num1|num2|num3|             strings|
     +----+----+----+--------------------+
     |   3|   4|   3|[[[[4:str4, 3:str...|
     |   4|   1|   3|[[[[1:str1, 4:str...|
     +----+----+----+--------------------+
    */
    assertThat(result.size()).isEqualTo(2);
    for (Row r : result) {
      assertThat(r.size()).isEqualTo(4);
    }
  }

  @Test
  public void testLeftAntiJoin() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    Dataset<Row> df =
        readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);

    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDatasetForJoin),
        testDataset.toString() + "." + testTable + "_to_join");
    Dataset<Row> df_to_join =
        readTestDataFromBigQuery(
            testDataset.toString(), testDataset.toString() + "." + testTable + "_to_join");

    // disabling pushdown to collect the join result to compare with pushdown enabled
    BigQueryConnectorUtils.disablePushdownSession(spark);
    List<Row> withoutPushDownResult =
        df.join(df_to_join, df.col("num1").equalTo(df_to_join.col("num1")), "leftanti")
            .collectAsList();

    // enabling pushdown to test join
    BigQueryConnectorUtils.enablePushdownSession(spark);
    List<Row> withPushDownResult =
        df.join(df_to_join, df.col("num1").equalTo(df_to_join.col("num1")), "leftanti")
            .collectAsList();

    // checking if the results with and without pushdown is the same
    assertThat(withPushDownResult.size()).isEqualTo(withoutPushDownResult.size());
    assertThat(withoutPushDownResult.containsAll(withPushDownResult)).isTrue();
    assertThat(withPushDownResult.containsAll(withoutPushDownResult)).isTrue();
    /*
     +----+----+----+-------+
     |num1|num2|num3|strings|
     +----+----+----+-------+
     +----+----+----+-------+
    */
    assertThat(withPushDownResult.size()).isEqualTo(0);

    // swapping the tables
    List<Row> result =
        df_to_join
            .join(df, df.col("num1").equalTo(df_to_join.col("num1")), "leftanti")
            .collectAsList();
    /*
     +----+----+----+--------------------+
     |num1|num2|num3|             strings|
     +----+----+----+--------------------+
     |   6|   5|   3|[[[[5:str5, 3:str...|
     +----+----+----+--------------------+
    */
    assertThat(result.size()).isEqualTo(1);
    assertThat(result.get(0).size()).isEqualTo(4);
    assertThat(result.get(0).get(0)).isEqualTo(6);
  }

  @Test
  public void testJoinQuery() {
    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDataset),
        testDataset.toString() + "." + testTable);
    Dataset<Row> df =
        readTestDataFromBigQuery(testDataset.toString(), testDataset.toString() + "." + testTable);
    df.createOrReplaceTempView("numStructDF");

    writeTestDataToBigQuery(
        getNumStructDataFrame(TestConstants.numStructDatasetForJoin),
        testDataset.toString() + "." + testTable + "_to_join");
    Dataset<Row> df_to_join =
        readTestDataFromBigQuery(
            testDataset.toString(), testDataset.toString() + "." + testTable + "_to_join");
    df_to_join.createOrReplaceTempView("numStructDF_to_join");

    String query =
        "SELECT "
            + "numStructDF.num1 AS a_num1, "
            + "numStructDF.num2 AS a_num2, "
            + "numStructDF.num3 AS a_num3, "
            + "numStructDF_to_join.num1 AS b_num1, "
            + "numStructDF_to_join.num2 AS b_num2, "
            + "numStructDF_to_join.num3 AS b_num3 "
            + "FROM numStructDF RIGHT JOIN numStructDF_to_join "
            + "ON numStructDF.num1 = numStructDF_to_join.num2 "
            + "WHERE numStructDF_to_join.num2 > 2 ";

    // disabling pushdown to collect the join result to compare with pushdown enabled
    BigQueryConnectorUtils.disablePushdownSession(spark);
    List<Row> withoutPushDownResult = spark.sql(query).collectAsList();

    // enabling pushdown to test join
    BigQueryConnectorUtils.enablePushdownSession(spark);
    List<Row> withPushDownResult = spark.sql(query).collectAsList();
    // checking if the results with and without pushdown is the same
    assertThat(withPushDownResult.size()).isEqualTo(withoutPushDownResult.size());
    assertThat(withoutPushDownResult.containsAll(withPushDownResult)).isTrue();
    assertThat(withPushDownResult.containsAll(withoutPushDownResult)).isTrue();

    /*
     +------+------+------+------+------+------+
     |a_num1|a_num2|a_num3|a_num2|b_num2|b_num3|
     +------+------+------+------+------+------+
     |  null|  null|  null|     6|     5|     3|
     |     4|     3|     2|     3|     4|     1|
     +------+------+------+------+------+------+
    */
    assertThat(withPushDownResult.size()).isEqualTo(2);
    for (Row r : withPushDownResult) {
      if (r.get(0) == null) {
        assertThat(r.get(4)).isEqualTo(5);
      } else {
        assertThat(r.get(0)).isEqualTo(r.get(4));
      }
    }
  }

  /**
   * Reading from a BigQuery table created with (note selects will not keep the order, so sort by
   * id):
   *
   * <p>create or replace table aiq-dev.connector_dev.dt (id integer, ts1 integer, ts2 integer, tz
   * string );
   *
   * <p>insert aiq-dev.connector_dev.dt values (0, unix_millis(timestamp("2023-09-01T23:59:59")),
   * unix_millis(timestamp("2023-09-02T00:00:00")),"UTC"), (1,
   * unix_millis(timestamp("2023-09-01T23:59:59")),
   * unix_millis(timestamp("2023-09-02T00:00:00")),"America/New_York"), (2,
   * unix_millis(timestamp("2023-09-01T23:59:59")),
   * unix_millis(timestamp("2023-09-02T00:00:00")),"Asia/Shanghai");
   */
  @Test
  public void testAiqDayDiff() {
    Dataset<Row> df = readTestDataFromBigQuery("connector_dev", "connector_dev.dt");
    df.createOrReplaceTempView("dt");
    List<Row> diffs1 =
        spark.sql("select aiq_day_diff(ts1, ts2, tz) from dt order by id").collectAsList();
    assertThat(diffs1.size()).isEqualTo(3);
    assertThat(diffs1.get(0).get(0)).isEqualTo(1);
    assertThat(diffs1.get(1).get(0)).isEqualTo(0);
    assertThat(diffs1.get(2).get(0)).isEqualTo(0);
    List<Row> diff2 =
        spark
            .sql("select aiq_day_diff(ts1, unix_timestamp() * 1000, 'UTC') from dt")
            .collectAsList();
    assert ((int) diff2.get(0).get(0) > 10); // 2023-09-01 to current
  }

  /**
   * Reading from a BigQuery table created with:
   *
   * <p>create or replace table aiq-dev.connector_dev.dt5 (date_str string, format string, tz
   * string)
   *
   * <p>insert into aiq-dev.connector_dev.dt5 values ('2019-09-01 14:50', 'yyyy-MM-dd HH:mm',
   * 'America/New_York'), ('2019-09-01 02:50 PM', 'yyyy-MM-dd hh:mm a', 'America/New_York'),
   * ('2019-09-01 PM 02:50', 'yyyy-MM-dd a hh:mm', 'America/New_York')
   */
  @Test
  public void testAiqStringToDate() {
    Dataset<Row> df = readTestDataFromBigQuery("connector_dev", "connector_dev.dt5");
    df.createOrReplaceTempView("dt5");

    List<Long> results =
        spark
            .sql(
                "select aiq_string_to_date(date_str, format, tz), date_str from dt5 order by date_str")
            .collectAsList().stream()
            .map(r -> r.getLong(0))
            .collect(Collectors.toList());

    assert (results.equals(Arrays.asList(1567363800000L, 1567363800000L, 1567363800000L)));
  }

  /**
   * Reading from a BigQuery table created with:
   *
   * <p>create or replace table aiq-dev.connector_dev.dt3 (ts integer )
   *
   * <p>insert into aiq-dev.connector_dev.dt3 values (1567363852000)
   */
  @Test
  public void testAiqDateToStringPart1() {
    var df = readTestDataFromBigQuery("connector_dev", "connector_dev.dt3");
    df.createOrReplaceTempView("dt3");

    var formatTestsWithIndex =
        zipWithIndex(
            Arrays.asList(
                "MM",
                "yyyy-MM-dd",
                "yyyy-MM-dd HH:mm",
                "yyyy-MM-dd hh:mm a",
                "yyyy-MM-dd a hh:mm",
                "yyyy-MM-dd a hh:mm:mm:ss a",
                "yyyy-MM-dd HH:mm:ss",
                "yyyy-MM-dd hh:mm:ss",
                "yyyy-MM-dd hh:mm:mm:ss",
                "yyyy-MM-dd M HH:mm:ss",
                "yyyy-MM-dd MM HH:mm:ss",
                "yyyy-MM-dd aMa HH:mm:ss",
                "yyyy-MM-dd MMM HH:mm:ss",
                "yyyy-MM-dd aMMMa HH:mm:ss",
                "yyyy-MM-dd MMMM HH:mm:ss",
                "yyyy-MM-dd MMMMM HH:mm:ss",
                "yyyy-MM-dd MMMMMM HH:mm:ss",
                "yyyy-MM-dd E HH:mm:ss",
                "yyyy-MM-dd EE HH:mm:ss",
                "yyyy-MM-dd EEE HH:mm:ss",
                "yyyy-MM-dd EEEE HH:mm:ss",
                "yyyy-MM-dd EEEEEEEE HH:mm:ss"));

    var sqlSelects =
        formatTestsWithIndex.stream()
            .map(
                f ->
                    "select "
                        + f.x()
                        + " as id, aiq_date_to_string(ts, '"
                        + f.y()
                        + "', 'America/New_York') as res from dt3")
            .collect(Collectors.toList());

    List<String> results =
        spark.sql(String.join(" UNION ALL ", sqlSelects) + " ORDER BY id").collectAsList().stream()
            .map(r -> r.getString(1))
            .collect(Collectors.toList());

    assert (results.equals(
        Arrays.asList(
            "09",
            "2019-09-01",
            "2019-09-01 14:50",
            "2019-09-01 02:50 PM",
            "2019-09-01 PM 02:50",
            "2019-09-01 PM 02:50:50:52 PM",
            "2019-09-01 14:50:52",
            "2019-09-01 02:50:52",
            "2019-09-01 02:50:50:52",
            "2019-09-01 09 14:50:52",
            "2019-09-01 09 14:50:52",
            "2019-09-01 PM09PM 14:50:52",
            "2019-09-01 Sep 14:50:52",
            "2019-09-01 PMSepPM 14:50:52",
            "2019-09-01 September 14:50:52",
            "2019-09-01 September 14:50:52",
            "2019-09-01 September 14:50:52",
            "2019-09-01 Sun 14:50:52",
            "2019-09-01 Sun 14:50:52",
            "2019-09-01 Sun 14:50:52",
            "2019-09-01 Sunday 14:50:52",
            "2019-09-01 Sunday 14:50:52")));
  }

  /**
   * Reading from a BigQuery table created with (note selects will not keep the order, so sort by
   * id):
   *
   * <p>create or replace table aiq-dev.connector_dev.dt2 (id integer, ts int64, fmt string, tz
   * string );
   *
   * <p>insert into aiq-dev.connector_dev.dt2 values (0, 1567363852000, 'MM', 'America/New_York'),
   * (1, 1567363852000, 'yyyy-MM-dd', 'America/New_York'), (2, 1567363852000, 'yyyy-MM-dd HH:mm',
   * 'America/New_York'), (3, 1567363852000, 'yyyy-MM-dd hh:mm a', 'America/New_York'), (4,
   * 1567363852000, 'yyyy-MM-dd a hh:mm', 'America/New_York'), (5, 1567363852000, 'yyyy-MM-dd a
   * hh:mm:mm:ss a', 'America/New_York'), (6, 1567363852000, 'yyyy-MM-dd HH:mm:ss',
   * 'America/New_York'), (7, 1567363852000, 'yyyy-MM-dd hh:mm:ss', 'America/New_York'), (8,
   * 1567363852000, 'yyyy-MM-dd hh:mm:mm:ss', 'America/New_York')
   */
  @Test
  public void testAiqDateToStringPart2() {
    Dataset<Row> df = readTestDataFromBigQuery("connector_dev", "connector_dev.dt2");
    df.createOrReplaceTempView("dt2");

    List<String> results =
        spark.sql("select aiq_date_to_string(ts, fmt, tz) from dt2 order by id").collectAsList()
            .stream()
            .map(r -> r.getString(0))
            .collect(Collectors.toList());

    assert (results.equals(
        Arrays.asList(
            "09",
            "2019-09-01",
            "2019-09-01 14:50",
            "2019-09-01 02:50 PM",
            "2019-09-01 PM 02:50",
            "2019-09-01 PM 02:50:50:52 PM",
            "2019-09-01 14:50:52",
            "2019-09-01 02:50:52",
            "2019-09-01 02:50:50:52")));
  }

  /**
   * Reading from a BigQuery table created with (note selects will not keep the order, so sort by
   * id):
   *
   * <p>create or replace table aiq-dev.connector_dev.dt4 (ts bigint, tz string, pd int)
   *
   * <p>insert into aiq-dev.connector_dev.dt4 values(1460080000000, 'America/New_York', 2) insert
   * into aiq-dev.connector_dev.dt4 values(1460080000000, 'Asia/Tokyo', -1)
   */
  @Test
  public void testAiqDayStart() {
    Dataset<Row> df = readTestDataFromBigQuery("connector_dev", "connector_dev.dt4");
    df.createOrReplaceTempView("dt4");

    List<Long> results =
        spark.sql("select aiq_day_start(ts, tz, pd), tz from dt4 order by tz").collectAsList()
            .stream()
            .map(r -> r.getLong(0))
            .collect(Collectors.toList());

    assert (results.equals(Arrays.asList(1460174400000L, 1459954800000L)));
  }

  /** Test for AIQ EXE-2026 */
  @Test
  public void testSourceQuery() {
    spark
        .sqlContext()
        .udf()
        .register(
            "appendStr",
            (String s) -> {
              return s + "abc";
            },
            DataTypes.StringType);
    Dataset<Row> df = readTestDataFromBigQuery("connector_dev", "connector_dev.basic");
    df.createOrReplaceTempView("t");
    Dataset<Row> df1 = spark.sql("select str_field from t limit 1");
    Dataset<Row> df2 = spark.sql("select * from t limit 1");
    Dataset<Row> df3 = spark.sql("select *, str_field from t limit 1");
    Dataset<Row> df4 = spark.sql("select num_field from t where appendStr(str_field) like 'a%'");
    SparkPlan sp1 = df1.queryExecution().sparkPlan();
    SparkPlan sp2 = df2.queryExecution().sparkPlan();
    SparkPlan sp3 = df3.queryExecution().sparkPlan();

    assertThat(sp1.toString()).contains("SELECT STR_FIELD FROM `connector_dev.basic`");
    // should select all columns instead of SELECT *
    assertThat(sp2.toString()).doesNotContain("SELECT * FROM `connector_dev.basic`");
    // select all columns from source table, the project all columns + str_field
    assertThat(sp3.toString()).doesNotMatch("SELECT \\*,[a-zA-Z_]* FROM `connector_dev.basic`");

    List<Row> res1 = df1.collectAsList();
    assertThat(res1.size()).isEqualTo(1);
    assertThat(res1.get(0).size()).isEqualTo(1);

    List<Row> res2 = df2.collectAsList();
    assertThat(res2.size()).isEqualTo(1);
    assertThat(res2.get(0).size()).isEqualTo(df.schema().size());

    List<Row> res4 = df4.collectAsList();
    assertThat(res4.size()).isEqualTo(1);
    assertThat(res4.get(0).size()).isEqualTo(1);
  }

  /** Creating a Dataset of NumStructType which will be used to write to BigQuery */
  protected Dataset<Row> getNumStructDataFrame(List<NumStruct> numStructList) {
    return spark.createDataset(numStructList, Encoders.bean(NumStruct.class)).toDF();
  }

  /** Method to create a test table of schema NumStruct, in test dataset */
  protected void writeTestDataToBigQuery(Dataset<Row> df, String table) {
    df.write()
        .format("bigquery")
        .mode(SaveMode.Append)
        .option("table", table)
        .option("writeMethod", WriteMethod.DIRECT.toString())
        .save();
  }

  /**
   * Method to read the test table from a BigQuery dataset
   *
   * @param dataset: the dataset where the table is located
   * @param table: in format of "dataset_name.table_name"
   */
  protected Dataset<Row> readTestDataFromBigQuery(String dataset, String table) {
    return spark
        .read()
        .format("bigquery")
        .option("materializationDataset", dataset)
        .option("viewsEnabled", true)
        .option("materializationExpirationTimeInMinutes", 30)
        .load(table);
  }
}
