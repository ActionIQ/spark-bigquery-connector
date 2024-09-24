package com.google.cloud.spark.bigquery.pushdowns

import com.google.cloud.bigquery.connector.common.BigQueryPushdownUnsupportedException
import com.google.cloud.spark.bigquery.pushdowns.SparkBigQueryPushdownUtil.{addAttributeStatement, blockStatement, makeStatement}
import org.apache.commons.lang.{StringEscapeUtils, StringUtils}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** This interface performs the conversion from Spark expressions to SQL runnable by BigQuery.
 * Spark Expressions are recursively pattern matched. Expressions that differ across Spark versions should be implemented in subclasses
 *
 */
abstract class SparkExpressionConverter {
  /**
   * Tries to convert Spark expressions by matching across the different families of expressions such as Aggregate, Boolean etc.
   * @param expression
   * @param fields
   * @return
   */
  def convertStatement(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement = {
    convertAggregateExpressions(expression, fields)
      .orElse(convertBasicExpressions(expression, fields))
      .orElse(convertBooleanExpressions(expression, fields))
      .orElse(convertDateExpressions(expression, fields))
      .orElse(convertMathematicalExpressions(expression, fields))
      .orElse(convertMiscellaneousExpressions(expression, fields))
      .orElse(convertStringExpressions(expression, fields))
      .orElse(convertWindowExpressions(expression, fields))
      .getOrElse(throw new BigQueryPushdownUnsupportedException((s"Pushdown unsupported for ${expression.prettyName}")))
  }

  def convertStatements(fields: Seq[Attribute], expressions: Expression*): BigQuerySQLStatement =
    makeStatement(expressions.map(convertStatement(_, fields)), ",")

  def convertAggregateExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    expression match {
      case _: AggregateExpression =>
        // Take only the first child, as all of the functions below have only one.
        expression.children.headOption.flatMap(agg_fun => {
          Option(agg_fun match {
            case _: HyperLogLogPlusPlus =>
              // NOTE: We are not passing through the other parameters in Spark's HLL
              // like mutableAggBufferOffset and inputAggBufferOffset
              ConstantString("APPROX_COUNT_DISTINCT") +
                blockStatement(convertStatements(fields, agg_fun.children: _*))

            case _: Average | _: Corr | _: CovPopulation | _: CovSample | _: Count |
                 _: Max | _: Min | _: Sum | _: StddevPop | _: StddevSamp |
                 _: VariancePop | _: VarianceSamp =>
              val distinct: BigQuerySQLStatement =
                if (expression.sql contains "(DISTINCT ") ConstantString("DISTINCT").toStatement
                else EmptyBigQuerySQLStatement()

              ConstantString(agg_fun.prettyName.toUpperCase) +
                blockStatement(
                  distinct + convertStatements(fields, agg_fun.children: _*)
                )
          })
        })
      case _ => None
    }
  }

  def convertBasicExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case a: Attribute => addAttributeStatement(a, fields)
      case And(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "AND" + convertStatement(right, fields)
        )
      case Or(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "OR" + convertStatement(right, fields)
        )
      case BitwiseAnd(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "&" + convertStatement(right, fields)
        )
      case BitwiseOr(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "|" + convertStatement(right, fields)
        )
      case BitwiseXor(left, right) =>
        blockStatement(
          convertStatement(left, fields) + "^" + convertStatement(right, fields)
        )
      case BitwiseNot(child) =>
        ConstantString("~") + blockStatement(
          convertStatement(child, fields)
        )
      case EqualNullSafe(left, right) =>

        /**
         * Since NullSafeEqual operator is not supported in BigQuery, we are instead converting the operator to COALESCE ( CAST (leftExpression AS STRING), "" ) = COALESCE ( CAST (rightExpression AS STRING), "" )
         * Casting the expression to String to make sure the COALESCE arguments are of same type.
         */
        blockStatement(
          ConstantString("COALESCE") + blockStatement( ConstantString("CAST") + blockStatement(convertStatement(left, fields) + ConstantString("AS STRING") ) + "," + ConstantString("\"\"") ) +
            ConstantString("=") +
            ConstantString("COALESCE") + blockStatement( ConstantString("CAST") + blockStatement(convertStatement(right, fields) + ConstantString("AS STRING") ) + "," + ConstantString("\"\"") )
        )

      // Bigquery does not support '%'
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/operators
      case Remainder(a, b, _) =>
        ConstantString("MOD") + blockStatement(
          convertStatement(Cast(a, LongType), fields) + "," + convertStatement(Cast(b, LongType), fields)
        )

      case b @ BinaryOperator(left, right) =>
        blockStatement(
          convertStatement(left, fields) + b.symbol + convertStatement(right, fields)
        )
      case l: Literal =>
        l.dataType match {
          case StringType =>
            if (l.value == null) {
              ConstantString("NULL").toStatement
            } else {
              StringVariable(Some(l.toString())).toStatement
            }
          case DateType =>
            ConstantString("DATE_ADD(DATE \"1970-01-01\", INTERVAL ") + IntVariable(
              Option(l.value).map(_.asInstanceOf[Int])
            ) + " DAY)" // s"DATE_ADD(DATE "1970-01-01", INTERVAL ${l.value} DAY)
          case TimestampType =>
            ConstantString("TIMESTAMP_MICROS(") + LongVariable(Option(l.value).map(_.asInstanceOf[Long])) + ")"
          case _ =>
            l.value match {
              case v: Int => IntVariable(Some(v)).toStatement
              case v: Long => LongVariable(Some(v)).toStatement
              case v: Short => ShortVariable(Some(v)).toStatement
              case v: Boolean => BooleanVariable(Some(v)).toStatement
              case v: Float => FloatVariable(Some(v)).toStatement
              case v: Double => DoubleVariable(Some(v)).toStatement
              case v: Byte => ByteVariable(Some(v)).toStatement
              case _ => ConstantStringVal(l.value).toStatement
            }
        }

      case _ => null
    })
  }

  def convertBooleanExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case In(child, list) =>
        convertStatement(child, fields) + "IN" +
          blockStatement(convertStatements(fields, list: _*))

      case IsNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NULL")

      case IsNotNull(child) =>
        blockStatement(convertStatement(child, fields) + "IS NOT NULL")

      case Not(child) => {
        child match {
          case EqualTo(left, right) =>
            blockStatement(
              convertStatement(left, fields) + "!=" +
                convertStatement(right, fields)
            )
          case GreaterThanOrEqual(left, right) =>
            convertStatement(LessThan(left, right), fields)
          case LessThanOrEqual(left, right) =>
            convertStatement(GreaterThan(left, right), fields)
          case GreaterThan(left, right) =>
            convertStatement(LessThanOrEqual(left, right), fields)
          case LessThan(left, right) =>
            convertStatement(GreaterThanOrEqual(left, right), fields)
          case _ =>
            ConstantString("NOT") +
              blockStatement(convertStatement(child, fields))
        }
      }

      case Contains(child, Literal(pattern: UTF8String, StringType)) =>
        ConstantString("CONTAINS_SUBSTR") + blockStatement(convertStatement(child, fields) + "," + s"'${pattern.toString.replace( "'", "\\'")}'")

      case EndsWith(child, Literal(pattern: UTF8String, StringType)) =>
        ConstantString("ENDS_WITH") + blockStatement(convertStatement(child, fields) + "," + s"'${pattern.toString.replace( "'", "\\'")}'")

      case StartsWith(child, Literal(pattern: UTF8String, StringType)) =>
        ConstantString("STARTS_WITH") + blockStatement(convertStatement(child, fields) + "," + s"'${pattern.toString.replace( "'", "\\'")}'")

      case _ => null
    })
  }

  def convertDateExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case DateAdd(startDate, days) =>
        ConstantString(expression.prettyName.toUpperCase) +
          blockStatement(
              convertStatement(startDate, fields) + ", INTERVAL " +
              convertStatement(days, fields) + "DAY"
          )
      case DateSub(startDate, days) =>
        ConstantString(expression.prettyName.toUpperCase) +
          blockStatement(
              convertStatement(startDate, fields) + ", INTERVAL " +
              convertStatement(days, fields) + "DAY"
          )
      case Month(child) =>
        ConstantString("EXTRACT") +
          blockStatement(
            ConstantString(expression.prettyName.toUpperCase) + " FROM " +
              convertStatement(child, fields)
          )
      case Quarter(child) =>
        ConstantString("EXTRACT") +
          blockStatement(
            ConstantString(expression.prettyName.toUpperCase) + " FROM " +
              convertStatement(child, fields)
          )
      case Year(child) =>
        ConstantString("EXTRACT") +
          blockStatement(
            ConstantString(expression.prettyName.toUpperCase) + " FROM " +
              convertStatement(child, fields)
          )
      case TruncDate(date, format) =>
        ConstantString("DATE_TRUNC") +
          blockStatement(
            convertStatement(date, fields) + s", ${format.toString()}"
          )

      /**
       * --- spark.sql(
       * ---   "select aiq_day_start(1460080000000, 'America/New_York', 2)"
       * --- ).as[Long].collect.head == 1460174400000L
       *
       * SELECT UNIX_MILLIS(
       *   TIMESTAMP(
       *     DATETIME_TRUNC(
       *       DATETIME_ADD(
       *         DATETIME(TIMESTAMP_MILLIS(1460080000000), 'America/New_York'),
       *         INTERVAL 2 DAY
       *       ),
     *         DAY
       *     ),
       *     'America/New_York'
       *   )
       * )
       * 1460174400000
       */
      case AiqDayStart(timestamp, timezone, plusDays) =>
        val tzStmt = convertStatement(timezone, fields)
        val innerTsm = ConstantString("TIMESTAMP_MILLIS") + blockStatement(convertStatement(timestamp, fields))
        val innerDt = ConstantString("DATETIME") + blockStatement(
          innerTsm + "," + tzStmt
        )
        val dtAdd = ConstantString("DATETIME_ADD") + blockStatement(
          innerDt + ", INTERVAL " + convertStatement(plusDays, fields) + " DAY"
        )
        val dtTrunc = ConstantString("DATETIME_TRUNC") + blockStatement(
          dtAdd + ", DAY"
        )
        val outerTs = ConstantString("TIMESTAMP") + blockStatement(
          dtTrunc + "," + tzStmt
        )
        ConstantString("UNIX_MILLIS") + blockStatement(outerTs)

      /**
       * --- spark.sql(
       * ---   """select aiq_day_diff(1693609200000, 1693616400000, 'UTC')"""
       * --- ).as[Long].collect.head == 1
       *
       * aiq_day_diff(startTsExpr, endTsExpr, timezoneExpr) =>
       * DATE_DIFF(
       *   DATE(TIMESTAMP_MILLIS(endTsExpr), timezoneExpr),
       *   DATE(TIMESTAMP_MILLIS(startTsExpr), timezoneExpr),
       *   DAY
       * )
       */
      case AiqDayDiff(startMs, endMs, timezoneId) =>
        val startTs = ConstantString("TIMESTAMP_MILLIS") + blockStatement(convertStatement(startMs, fields))
        val startDt = ConstantString("DATE") + blockStatement(startTs + "," + convertStatement(timezoneId, fields))
        val endTs = ConstantString("TIMESTAMP_MILLIS") + blockStatement(convertStatement(endMs, fields))
        val endDt = ConstantString("DATE") + blockStatement(endTs + "," + convertStatement(timezoneId, fields))
        ConstantString("DATE_DIFF") + blockStatement(endDt + "," + startDt + "," + "DAY")

      /**
       * --- spark.sql(
       * ---   """select aiq_date_to_string(1567363852000, "yyyy-MM-dd HH:mm", 'America/New_York')"""
       * --- ).as[String].collect.head == "2019-09-01 14:50"
       *
       * SELECT CAST(DATETIME(TIMESTAMP_MILLIS(1567363852000) , "America/New_York") AS STRING FORMAT "yyyy-MM-dd HH24:mi")
       * 2019-09-01 14:50
       */
      case AiqDateToString(timestamp, dateFormat, timezoneId) if dateFormat.foldable =>
        val tsMillisStmt = ConstantString("TIMESTAMP_MILLIS") + blockStatement(convertStatement(timestamp, fields))
        val datetimeStmt = ConstantString("DATETIME") + blockStatement(tsMillisStmt + "," + convertStatement(timezoneId, fields))
        val fixedFormat = isoDateFmtToBigQueryFormat(dateFormat.toString)
        val formatStr = s"""AS STRING FORMAT "$fixedFormat""""
        ConstantString("CAST") + blockStatement(datetimeStmt + formatStr)

      /**
       * --- spark.sql(
       * ---   "select aiq_string_to_date('2019-09-01 14:50:52', 'yyyy-MM-dd HH:mm:ss', 'America/New_York')"
       * --- ).as[Long].collect.head == 1567363852000L
       *
       * SELECT UNIX_MILLIS(
       *   TIMESTAMP(
       *     PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2019-09-01 14:50:52'),
       *     'America/New_York'))
       *
       * -- 1567363852000
       */
      case AiqStringToDate(dateStr, format, timezone) if format.foldable =>
        val newFormat = isoDateFmtToBigQueryParse(format.toString)
        val parsedDt = ConstantString("PARSE_DATETIME") + blockStatement(
           convertStatement(Literal(newFormat), fields) + "," + convertStatement(dateStr, fields)
        )
        val timestampInZone = ConstantString("TIMESTAMP") + blockStatement(
          parsedDt + "," + convertStatement(timezone, fields)
        )
        ConstantString("UNIX_MILLIS") + blockStatement(timestampInZone)

      case _ => null
    })
  }

  /**
   * Function to convert a ISO date format:
   *
   * https://en.wikipedia.org/wiki/ISO_8601#Times
   *
   * To Bigquery parse language:
   *
   * https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_date_time_as_string
   */
  private def isoDateFmtToBigQueryParse(format: String): String = {
    // be careful with the order here, you dont want mmM -> %MM -> %%m
    val formatMap = Seq(
      "YYYY" -> "%Y", // Four-digit year
      "yyyy" -> "%Y", // Four-digit year (alternative)
      "YY" -> "%y",   // Two-digit year
      "MM" -> "%m",   // Two-digit month
      "dd" -> "%d",   // Two-digit day
      "DD" -> "%d",   // Two-digit day
      "HH" -> "%H",   // Two-digit hour (24-hour format)
      "hh" -> "%I",   // Two-digit hour (12-hour format)
      "mm" -> "%M",   // Two-digit minute
      "ss" -> "%S",   // Two-digit second
      "SSS" -> "%f",  // Milliseconds
      "Z" -> "%Z"     // UTC offset
    )

    // Replace ISO 8601 specifiers with BigQuery specifiers
    val transformed = formatMap.foldLeft(format) { case (last, (nextFmtKey, nextFmtVal)) =>
      last.replace(nextFmtKey, nextFmtVal)
    }

    // Handle fractional seconds
    transformed.replace(".S", ".%f")
  }

  /**
   * Function to convert a ISO date format:
   *
   * https://en.wikipedia.org/wiki/ISO_8601#Times
   *
   * To Bigquery format language:
   *
   * https://cloud.google.com/bigquery/docs/reference/standard-sql/format-elements#format_date_time_as_string
   */
  private def isoDateFmtToBigQueryFormat(format: String): String = {
    // be careful with the order here, you dont want MMM -> Month -> MMonth
    format
      // Two-digit month => M -> MM
      .replaceAll("(?<=[^M])M(?=[^M])", "MM")
      // Full month name => MMMM... -> Month
      .replaceAll("(?<=[^M])M{4,8}(?=[^M])", "Month")
      // Abbreviated month name => MMM -> MON
      .replaceAll("(?<=[^M])M{3}(?=[^M])", "Mon")
      // Two digits for hour (00 through 23)
      .replaceAll("HH", "HH24")
      // Two digits for hour (01 through 12)
      .replaceAll("hh", "HH12")
      // Two digits for minute (00 through 59)
      .replaceAll("mm", "MI")
      // Two digits for second (00 through 59)
      .replaceAll("ss", "SS")
      // Ante meridiem (am) / post meridiem (pm)
      .replaceAll("a", "AM")
      .replaceAll("p", "PM")
      // Full day of week => EEEE... -> Day
      .replaceAll("(?<=[^E])E{4,8}(?=[^E])", "Day")
      // Abbreviated day of week => E/EE/EEE -> Dy
      .replaceAll("(?<=[^E])E{1,3}(?=[^E])", "Dy")
  }

  def convertMathematicalExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case _: Abs | _: Acos | _: Asin | _: Atan |
           _: Cos | _: Cosh | _: Exp | _: Floor | _: Greatest |
           _: Least | _:Log10 | _: Pow | _:Round | _: Sin | _: Sinh |
           _: Sqrt | _: Tan | _: Tanh =>
        ConstantString(expression.prettyName.toUpperCase) + blockStatement(convertStatements(fields, expression.children: _*))

      /**
       * Only supporting 16 -> 10 for now
       *
       *  spark.sql("select conv('fd33e2e8ad', 16, 10)").collect
       *  1087497234605
       *
       *  SELECT CAST(CAST(CONCAT('0x', 'fd33e2e8ad') AS INT64) AS STRING)
       */
      case Conv(numberStr, fromBase, toBase) if
          fromBase.foldable && fromBase.toString == "16" && toBase.foldable && toBase.toString == "10" =>
        val newExp = Cast(
          Cast(
            Concat(Seq(Literal("0x"), numberStr)),
            LongType
          ),
          StringType
        )
        convertStatement(newExp, fields)

      // These hash functions all return bytes, so must convert to hex string

      // SELECT TO_HEX(MD5("Spark"))
      // 8cde774d6f7333752ed72cacddb05126
      case Md5(child) =>
        val hashRes = ConstantString("MD5") + blockStatement(convertStatement(child, fields))
        ConstantString("TO_HEX") + blockStatement(hashRes)

      // SELECT sha1('Spark')
      // 85f5955f4b27a9a4c2aab6ffe5d7189fc298b92c
      case Sha1(child) =>
        val hashRes = ConstantString("SHA1") + blockStatement(convertStatement(child, fields))
        ConstantString("TO_HEX") + blockStatement(hashRes)

      // BQ only supports sha 256:
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/hash_functions#sha256
      //
      // SELECT sha2('Spark', 256)
      // 529bc3b07127ecb7e53a4dcf1991d9152c24537d919178022b2c42657f79a26b
      case Sha2(child, bitLen) if bitLen.foldable && bitLen.toString == "256" =>
        val hashRes = ConstantString("SHA256") + blockStatement(convertStatement(child, fields))
        ConstantString("TO_HEX") + blockStatement(hashRes)

      case IsNaN(child) =>
        ConstantString("IS_NAN") + blockStatement(convertStatement(child, fields))

      case Signum(child) =>
        ConstantString("SIGN") + blockStatement(convertStatement(child, fields))

      case _: Rand =>
        ConstantString("RAND") + ConstantString("()")

      case Logarithm(base, expr) =>
        // In spark it is LOG(base,expr) whereas in BigQuery it is LOG(expr, base)
        ConstantString("LOG") + blockStatement(convertStatement(expr, fields) + "," + convertStatement(base, fields))

      case _: CheckOverflow =>
        convertCheckOverflowExpression(expression, fields)

      case Pi() => ConstantString("bqutil.fn.pi()").toStatement

      case PromotePrecision(child) => convertStatement(child, fields)

      case _: UnaryMinus =>
        convertUnaryMinusExpression(expression, fields)

      case _ => null
    })
  }

  def convertMiscellaneousExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case Alias(child: Expression, name: String) =>
        blockStatement(convertStatement(child, fields), name)

      case SortOrder(child, Ascending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "ASC"

      case SortOrder(child, Descending, _, _) =>
        blockStatement(convertStatement(child, fields)) + "DESC"

      case ShiftLeft(child, position) =>
        blockStatement(convertStatement(child, fields) + ConstantString("<<") + convertStatement(position, fields))

      case ShiftRight(child, position) =>
        blockStatement(convertStatement(child, fields) + ConstantString(">>") + convertStatement(position, fields))

      case CaseWhen(branches, elseValue) =>
        ConstantString("CASE") +
          makeStatement(branches.map(whenClauseTuple =>
            ConstantString("WHEN") + convertStatement(whenClauseTuple._1, fields) + ConstantString("THEN") + convertStatement(whenClauseTuple._2, fields)
          ), "") +
          {
            elseValue match {
              case Some(value) =>
                ConstantString("ELSE") + convertStatement(value, fields)
              case None =>
                EmptyBigQuerySQLStatement()
            }
          } + ConstantString("END")

      case Coalesce(columns) =>
        ConstantString(expression.prettyName.toUpperCase) + blockStatement(makeStatement(columns.map(convertStatement(_, fields)), ", "))

      case If(predicate, trueValue, falseValue) =>
        ConstantString(expression.prettyName.toUpperCase) + blockStatement(convertStatements(fields, predicate, trueValue, falseValue))

      case InSet(child, hset) =>
        convertStatement( In(child, setToExpression(hset)), fields)

      case UnscaledValue(child) =>
        child.dataType match {
          case d: DecimalType =>
            blockStatement(convertStatement(child, fields) + "* POW( 10," + IntVariable(Some(d.scale)) + ")")
          case _ => null
        }

      case _: Cast =>
        convertCastExpression(expression, fields)

      case _: ScalarSubquery =>
        convertScalarSubqueryExpression(expression, fields)

      case _ => null
    })
  }

  private def isIntegralType(dt: DataType): Boolean = dt.isInstanceOf[LongType] || dt.isInstanceOf[IntegerType]

  def convertStringExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case _: Ascii | _: Concat | _: Length | _: Lower |
           _: StringLPad | _: StringRPad | _: StringTranslate |
           _: StringTrim | _: StringTrimLeft | _: StringTrimRight |
           _: Upper | _: StringInstr | _: InitCap |
           _: Substring =>
        ConstantString(expression.prettyName.toUpperCase()) + blockStatement(convertStatements(fields, expression.children: _*))
      case _: Like =>
        convertLikeExpression(expression, fields)
      case RegExpExtract(child, Literal(pattern: UTF8String, StringType), idx) =>
        ConstantString("REGEXP_EXTRACT") + blockStatement(convertStatement(child, fields) + "," + s"r'${pattern.toString}'" + "," + convertStatement(idx, fields))
     case _: RegExpReplace =>
        ConstantString("REGEXP_REPLACE") + blockStatement(convertStatement(expression.children.head, fields) + "," + s"r'${expression.children(1).toString}'" + "," + s"'${expression.children(2).toString}'")

      /**
       * Only support reducing decimal places, not full formatting, and special casing
       * decimalPlaces=0 to make logic simpler
       *
       * spark.sql("SELECT format_number(12332.123456, 4)").collect
       * -- 12,332.1235
       * scala> spark.sql("select format_number(123, 4)").collect
       * -- 123.0000
       * scala> spark.sql("select format_number(4567.123, 0)").collect
       * -- 4,567
       *
       * select CONCAT(
       *     FORMAT("%'d", CAST('12332.123456' AS INT64)),
       *     SUBSTRING(FORMAT("%.4f", MOD(CAST('12332.123456' AS NUMERIC), 1)), 2, 5)
       * )
       */
      case FormatNumber(numberExp, decimalPlacesExp)
          if decimalPlacesExp.foldable && isIntegralType(decimalPlacesExp.dataType) && decimalPlacesExp.toString == "0" =>
        val firstPart = FormatString(Literal("%\\\'d"), Cast(numberExp, LongType))
        convertStatement(firstPart, fields)

      case FormatNumber(numberExp, decimalPlacesExp)
          if decimalPlacesExp.foldable && isIntegralType(decimalPlacesExp.dataType) =>
        val firstPart = FormatString(Literal("%\\\'d"), Cast(numberExp, LongType))
        val secondPart = Substring(
          FormatString(Literal("%.4f"), Remainder(numberExp, Literal(1L))),
          Literal(2),
          Literal(decimalPlacesExp.toString.toInt + 1)
        )
        convertStatement(Concat(Seq(firstPart, secondPart)), fields)

      case _: FormatString =>
        ConstantString("FORMAT") + blockStatement(convertStatements(fields, expression.children: _*))
      case _: Base64 =>
        ConstantString("TO_BASE64") + blockStatement(convertStatements(fields, expression.children: _*))
      case _: UnBase64 =>
        ConstantString("FROM_BASE64") + blockStatement(convertStatements(fields, expression.children: _*))
      case _: SoundEx =>
        ConstantString("UPPER") + blockStatement(ConstantString("SOUNDEX") + blockStatement(convertStatements(fields, expression.children: _*)))
      case _ => null
    })
  }

  def convertWindowExpressions(expression: Expression, fields: Seq[Attribute]): Option[BigQuerySQLStatement] = {
    Option(expression match {
      case WindowExpression(windowFunction, windowSpec) =>
        windowFunction match {
          /**
           * Since we can't use a window frame clause with navigation functions and numbering functions,
           * we set the useWindowFrame to false
           */
          case _: Rank | _: DenseRank | _: PercentRank | _: RowNumber =>
            convertStatement(windowFunction, fields) +
            ConstantString("OVER") +
            convertWindowBlock(windowSpec, fields, generateWindowFrame = false)
          case _ =>
            convertStatement(windowFunction, fields) +
              ConstantString("OVER") +
              convertWindowBlock(windowSpec, fields, generateWindowFrame = true)
        }
      /**
       * Handling Numbering Functions here itself since they are a sub class of Window Expressions
       */
      case _: Rank | _: DenseRank | _: PercentRank | _: RowNumber =>
        ConstantString(expression.prettyName.toUpperCase) + ConstantString("()")
      case _ => null
    })
  }

  def convertWindowBlock(windowSpecDefinition: WindowSpecDefinition, fields: Seq[Attribute], generateWindowFrame: Boolean): BigQuerySQLStatement = {
    val partitionBy =
      if (windowSpecDefinition.partitionSpec.nonEmpty) {
        ConstantString("PARTITION BY") +
          makeStatement(windowSpecDefinition.partitionSpec.map(convertStatement(_, fields)), ",")
      } else {
        EmptyBigQuerySQLStatement()
      }

    val orderBy =
      if (windowSpecDefinition.orderSpec.nonEmpty) {
        ConstantString("ORDER BY") +
          makeStatement(windowSpecDefinition.orderSpec.map(convertStatement(_, fields)), ",")
      } else {
        EmptyBigQuerySQLStatement()
      }

    /**
     * Generating the window frame iff generateWindowFrame is true and the window spec has order spec in it
     */
    val windowFrame =
      if (generateWindowFrame && windowSpecDefinition.orderSpec.nonEmpty) {
        windowSpecDefinition.frameSpecification match {
          case windowSpec: SpecifiedWindowFrame =>
          (windowSpec.lower, windowSpec.upper) match {
            case (lower: Literal, upper: Literal) =>
              generateWindowFrameFromSpecDefinition(windowSpec, Math.abs(lower.value.asInstanceOf[Long]).toString + " " + ConstantString("PRECEDING"), Math.abs(upper.value.asInstanceOf[Long]).toString + " " + ConstantString("FOLLOWING"))
            case (lower: Literal, upper: SpecialFrameBoundary) =>
              generateWindowFrameFromSpecDefinition(windowSpec, Math.abs(lower.value.asInstanceOf[Long]).toString + " " + ConstantString("PRECEDING"), upper.sql)
            case (lower: SpecialFrameBoundary, upper: Literal) =>
              generateWindowFrameFromSpecDefinition(windowSpec, lower.sql, Math.abs(upper.value.asInstanceOf[Long]).toString + " " + ConstantString("FOLLOWING"))
            case _ =>
              windowSpecDefinition.frameSpecification.sql
          }
          case _ =>
            windowSpecDefinition.frameSpecification.sql
        }
      } else {
        ""
      }

    blockStatement(partitionBy + orderBy + windowFrame)
  }

  def generateWindowFrameFromSpecDefinition(windowSpec: SpecifiedWindowFrame, lower: String, upper: String): String = {
    windowSpec.frameType.sql + " " + ConstantString("BETWEEN") + " " + lower + " " + ConstantString("AND") + " " + upper
  }

  /** Attempts a best effort conversion from a SparkType
   * to a BigQuery type to be used in a Cast.
   */
  final def getCastType(t: DataType): Option[String] =
    Option(t match {
      case StringType => "STRING"
      case ByteType => "BYTES"
      case BooleanType => "BOOL"
      case DateType => "DATE"
      case TimestampType => "TIMESTAMP"
      case _: DecimalType => "BIGDECIMAL"
      case IntegerType | ShortType | LongType => "INT64"
      case FloatType | DoubleType => "FLOAT64"
      case _ => null
    })

  final def performCastExpressionConversion(child: Expression, fields: Seq[Attribute], dataType: DataType): BigQuerySQLStatement =
    getCastType(dataType) match {
      case Some(cast) =>

        /**
         * For known unsupported data conversion, raise exception to break the pushdown process.
         * For example, BigQuery doesn't support to convert DATE/TIMESTAMP to NUMBER
         */
        (child.dataType, dataType) match {
          case (_: DateType | _: TimestampType,
          _: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType) => {
            throw new BigQueryPushdownUnsupportedException(
              "Pushdown failed due to unsupported conversion")
          }

          /**
           * BigQuery doesn't support casting from Integer to Bytes (https://cloud.google.com/bigquery/docs/reference/standard-sql/functions-and-operators#cast_as_bytes)
           * So handling this case separately.
           */
          case (_: IntegerType | _: LongType | _: FloatType | _: DoubleType | _: DecimalType ,_: ByteType) =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + ConstantString("AS NUMERIC"))
          case _ =>
            ConstantString("CAST") +
              blockStatement(convertStatement(child, fields) + "AS" + cast)
        }

      case _ => convertStatement(child, fields)
    }

  final def setToExpression(set: Set[Any]): Seq[Expression] = {
    set.map {
      case d: Decimal => Literal(d, DecimalType(d.precision, d.scale))
      case s @ (_: String | _: UTF8String) => Literal(s, StringType)
      case d: Double => Literal(d, DoubleType)
      case l: Long => Literal(l, LongType)
      case e: Expression => e
      case default =>
        throw new BigQueryPushdownUnsupportedException(
          "Pushdown unsupported for " + s"${default.getClass.getSimpleName} @ MiscStatement.setToExpression"
        )
    }.toSeq
  }

  // For supporting Scalar Subquery, we need specific implementations of BigQueryStrategy
  def convertScalarSubqueryExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement

  def convertCheckOverflowExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement

  def convertUnaryMinusExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement

  def convertCastExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement

  def convertLikeExpression(expression: Expression, fields: Seq[Attribute]): BigQuerySQLStatement
}
