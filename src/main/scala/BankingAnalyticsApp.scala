import java.nio.file.Paths

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DecimalType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window

object BankingAnalyticsApp {
  def main(args: Array[String]): Unit = {
    val dataRoot = Paths.get("data").toAbsolutePath
    val customerPath = dataRoot.resolve("customer/customer.csv").toString
    val transactionPath = dataRoot.resolve("transaction/transactions.csv").toString
    val branchPath = dataRoot.resolve("branch/branches.csv").toString
    val loanPath = dataRoot.resolve("loan/loans.csv").toString
    val outputRoot = dataRoot.resolve("output").toString

    val spark = SparkSession.builder()
      .appName("Banking Customer 360 Demo")
      .master("local[*]")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try {
      val customerSchema = StructType(Seq(
        StructField("customer_id", StringType, nullable = false),
        StructField("customer_name", StringType, nullable = true),
        StructField("city", StringType, nullable = true),
        StructField("account_type", StringType, nullable = false)
      ))
      val transactionSchema = StructType(Seq(
        StructField("transaction_id", StringType, nullable = false),
        StructField("customer_id", StringType, nullable = false),
        StructField("transaction_type", StringType, nullable = true),
        StructField("amount", DecimalType(18, 2), nullable = true),
        StructField("transaction_status", StringType, nullable = true)
      ))

      val customers = spark.read.option("header", "true").schema(customerSchema).csv(customerPath)
      val transactions = spark.read.option("header", "true").schema(transactionSchema).csv(transactionPath)
      val branches = spark.read.option("header", "true").schema(BranchSchema.schema).csv(branchPath)
      val loans = spark.read.option("header", "true").schema(LoanSchema.schema).csv(loanPath)

      // Aggregate each fact table before joining, so multiple loans do not inflate transaction totals.
      val transactionMetrics = transactions
        .withColumn("normalized_status", upper(trim(col("transaction_status"))))
        .groupBy("customer_id")
        .agg(
          sum(when(col("normalized_status") === "SUCCESS", col("amount")).otherwise(lit(0).cast(DecimalType(18, 2))))
            .alias("successful_volume"),
          count(lit(1)).alias("transaction_count"),
          sum(when(col("normalized_status") === "FAILED", 1).otherwise(0)).alias("failed_transaction_count"),
          max(when(col("normalized_status") === "SUCCESS", col("amount"))).alias("largest_successful_transaction")
        )

      val loanMetrics = loans
        .withColumn("normalized_status", upper(trim(col("loan_status"))))
        .groupBy("customer_id")
        .agg(
          count(col("loan_id")).alias("loan_count"),
          sum(col("principal_amount")).alias("original_loan_amount"),
          sum(col("outstanding_amount")).alias("outstanding_loan_amount"),
          sum(when(col("normalized_status") === "OVERDUE", 1).otherwise(0)).alias("overdue_loan_count")
        )

      val customer360 = customers
        .join(broadcast(branches), Seq("city"), "left")
        .join(transactionMetrics, Seq("customer_id"), "left")
        .join(loanMetrics, Seq("customer_id"), "left")
        .na.fill(Map(
          "successful_volume" -> 0,
          "transaction_count" -> 0,
          "failed_transaction_count" -> 0,
          "loan_count" -> 0,
          "original_loan_amount" -> 0,
          "outstanding_loan_amount" -> 0,
          "overdue_loan_count" -> 0
        ))
        .withColumn("largest_successful_transaction", coalesce(col("largest_successful_transaction"), lit(0)))
        .withColumn("risk_band",
          when(col("overdue_loan_count") > 0 || col("failed_transaction_count") >= 2, "HIGH")
            .when(col("failed_transaction_count") > 0 || col("outstanding_loan_amount") >= 1000000, "MEDIUM")
            .otherwise("LOW"))
        .withColumn("activity_segment",
          when(col("transaction_count") >= 3, "Highly Active")
            .when(col("transaction_count") >= 1, "Active")
            .otherwise("No Transactions"))
        .withColumn("branch_customer_rank",
          dense_rank().over(Window.partitionBy("branch_id").orderBy(desc("successful_volume"))))
        .select(
          col("customer_id"), col("customer_name"), col("city"), col("account_type"),
          col("branch_id"), col("branch_name"), col("region"), col("service_tier"),
          col("successful_volume"), col("transaction_count"), col("failed_transaction_count"),
          col("largest_successful_transaction"), col("loan_count"), col("original_loan_amount"),
          col("outstanding_loan_amount"), col("overdue_loan_count"), col("risk_band"),
          col("activity_segment"), col("branch_customer_rank")
        )

      val branchPerformance = customer360
        .groupBy("branch_id", "branch_name", "city", "region", "service_tier")
        .agg(
          countDistinct("customer_id").alias("customer_count"),
          sum("successful_volume").alias("successful_volume"),
          sum("transaction_count").alias("transaction_count"),
          sum("outstanding_loan_amount").alias("outstanding_loan_amount"),
          sum(when(col("risk_band") === "HIGH", 1).otherwise(0)).alias("high_risk_customers")
        )

      println("\n=== CUSTOMER 360: transaction, branch and lending profile ===")
      customer360.orderBy(col("customer_id")).show(false)
      println("\n=== BRANCH SCORECARD ===")
      branchPerformance.orderBy(desc("successful_volume")).show(false)

      /*if (branchPerformance.count() < 10){
        throw new Error("branch count shoud be greated than 10")
      }*/

      customer360.write.mode("overwrite").option("header", "true")
        .csv(Paths.get(outputRoot, "customer_360").toString)
      branchPerformance.write.mode("overwrite").option("header", "true")
        .csv(Paths.get(outputRoot, "branch_scorecard").toString)

      println(s"Demo complete. Reports written beneath: $outputRoot")
    } finally {
      spark.stop()
    }
  }
}
