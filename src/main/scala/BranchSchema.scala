import org.apache.spark.sql.types.{StringType, StructField, StructType}

object BranchSchema {
  val schema: StructType = StructType(Seq(
    StructField("branch_id", StringType, nullable = false),
    StructField("branch_name", StringType, nullable = false),
    StructField("city", StringType, nullable = false),
    StructField("region", StringType, nullable = false),
    StructField("service_tier", StringType, nullable = false)
  ))
}
