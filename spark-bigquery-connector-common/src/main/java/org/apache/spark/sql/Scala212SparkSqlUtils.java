package org.apache.spark.sql;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer$;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;
import scala.collection.mutable.ListBuffer;

/** Copy-pasted from [[PreScala213SparkSqlUtils]] */
public class Scala212SparkSqlUtils extends SparkSqlUtils {
  @Override
  public boolean supportsScalaVersion(String scalaVersion) {
    return scalaVersion.compareTo("2.13") < 0;
  }

  @Override
  public InternalRow rowToInternalRow(Row row) {
    return InternalRow.fromSeq(row.toSeq());
  }

  // This method relies on the scala.Seq alias, which is different in Scala 2.12 and 2.13. In Scala
  // 2.12 scala.Seq points to scala.collection.Seq whereas in Scala 2.13 it points to
  // scala.collection.immutable.Seq.   @Override
  public ExpressionEncoder<Row> createExpressionEncoder(StructType schema) {
    List<Attribute> attributes =
        JavaConverters.asJavaCollection(toAttributes(schema)).stream()
            .map(Attribute::toAttribute)
            .collect(Collectors.toList());
    ExpressionEncoder<Row> expressionEncoder =
        RowEncoder.apply(schema)
            .resolveAndBind(
                JavaConverters.asScalaIteratorConverter(attributes.iterator()).asScala().toSeq(),
                SimpleAnalyzer$.MODULE$);
    return expressionEncoder;
  }

  // `toAttributes` is protected[sql] starting spark 3.2.0, so we need this call to be in the same
  // package. Since Scala 2.13/Spark 3.3 forbids it, the implementation has been ported to Java
  public static scala.collection.Seq<AttributeReference> toAttributes(StructType schema) {
    List<AttributeReference> result =
        Stream.of(schema.fields())
            .map(
                field ->
                    new AttributeReference(
                        field.name(),
                        field.dataType(),
                        field.nullable(),
                        field.metadata(),
                        NamedExpression.newExprId(),
                        new ListBuffer<String>().toSeq()))
            .collect(Collectors.toList());
    return JavaConverters.asScalaBuffer(result).toSeq();
  }
}
