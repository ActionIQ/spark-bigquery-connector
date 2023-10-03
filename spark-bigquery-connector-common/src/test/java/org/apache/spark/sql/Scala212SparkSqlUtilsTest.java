package org.apache.spark.sql;

import static com.google.common.truth.Truth.assertThat;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.Test;

/** Copy-pasted from [[PreScala213SparkSqlUtilsTest]] */
public class Scala212SparkSqlUtilsTest {
  @Test
  public void testRowToInternalRow() throws Exception {
    SparkSqlUtils ssu = SparkSqlUtils.getInstance();
    assertThat(ssu).isInstanceOf(Scala212SparkSqlUtils.class);
    Row row = new GenericRow(new Object[] {UTF8String.fromString("a"), 1});
    InternalRow internalRow = ssu.rowToInternalRow(row);
    assertThat(internalRow.numFields()).isEqualTo(2);
    assertThat(internalRow.getString(0).toString()).isEqualTo("a");
    assertThat(internalRow.getInt(1)).isEqualTo(1);
  }
}
