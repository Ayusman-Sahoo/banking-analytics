import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}

object LoanSchema {
  val schema: StructType = StructType(Seq(
    StructField("loan_id", StringType, nullable = false),
    StructField("customer_id", StringType, nullable = false),
    StructField("loan_type", StringType, nullable = false),
    StructField("principal_amount", DecimalType(18, 2), nullable = false),
    StructField("outstanding_amount", DecimalType(18, 2), nullable = false),
    StructField("loan_status", StringType, nullable = false)
  ))
}
