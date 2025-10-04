# Databricks notebook source
# MAGIC %md
# MAGIC # Quality Integration: Great Expectations & Deequ
# MAGIC 
# MAGIC This notebook demonstrates:
# MAGIC 1. Data quality validation with Great Expectations
# MAGIC 2. Automated quality checks with Deequ
# MAGIC 3. Data profiling and anomaly detection
# MAGIC 4. Quality monitoring and alerting
# MAGIC 5. Integration with Delta Lake workflows
# MAGIC 6. Quality gates for data pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Install required packages
%pip install great-expectations deequ

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from datetime import datetime
import pandas as pd

# Storage configuration
storage_account = "voodatabricks10344"
curated_container = "curated"
curated_path = f"abfss://{curated_container}@{storage_account}.dfs.core.windows.net/"

print(f"Curated path: {curated_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Sample Data for Quality Testing

# COMMAND ----------

# Create sample dataset with quality issues for testing
def create_sample_data_with_issues():
    """Create sample data with various quality issues"""
    
    sample_data = [
        # Valid records
        {"customer_id": "CUST001", "email": "john@example.com", "age": 25, "salary": 50000, "city": "New York"},
        {"customer_id": "CUST002", "email": "jane@example.com", "age": 30, "salary": 60000, "city": "Los Angeles"},
        {"customer_id": "CUST003", "email": "bob@example.com", "age": 35, "salary": 70000, "city": "Chicago"},
        {"customer_id": "CUST004", "email": "alice@example.com", "age": 28, "salary": 55000, "city": "Houston"},
        {"customer_id": "CUST005", "email": "charlie@example.com", "age": 32, "salary": 65000, "city": "Phoenix"},
        
        # Quality issues
        {"customer_id": "CUST006", "email": "invalid-email", "age": 25, "salary": 50000, "city": "New York"},  # Invalid email
        {"customer_id": "CUST007", "email": "sarah@example.com", "age": -5, "salary": 50000, "city": "Boston"},  # Negative age
        {"customer_id": "CUST008", "email": "mike@example.com", "age": 150, "salary": 50000, "city": "Seattle"},  # Unrealistic age
        {"customer_id": "CUST009", "email": "lisa@example.com", "age": 30, "salary": -1000, "city": "Denver"},  # Negative salary
        {"customer_id": "CUST010", "email": "tom@example.com", "age": 45, "salary": 50000, "city": None},  # Null city
        {"customer_id": "CUST011", "email": None, "age": 40, "salary": 50000, "city": "Miami"},  # Null email
        {"customer_id": None, "email": "kate@example.com", "age": 35, "salary": 50000, "city": "Atlanta"},  # Null customer_id
        {"customer_id": "CUST012", "email": "duplicate@example.com", "age": 29, "salary": 50000, "city": "Portland"},  # Duplicate email
        {"customer_id": "CUST013", "email": "duplicate@example.com", "age": 31, "salary": 50000, "city": "Las Vegas"},  # Duplicate email
    ]
    
    return spark.createDataFrame(sample_data)

# Create sample data
df_sample = create_sample_data_with_issues()

# Write to Delta table
quality_test_path = f"{curated_path}quality/customer_data_quality_test"
df_sample.write.format("delta").mode("overwrite").save(quality_test_path)

# Create table
spark.sql(f"""
CREATE TABLE IF NOT EXISTS customer_data_quality_test
USING DELTA
LOCATION '{quality_test_path}'
""")

print("Sample data with quality issues created:")
df_sample.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Great Expectations Integration

# COMMAND ----------

# Import Great Expectations
import great_expectations as ge
from great_expectations.core import ExpectationSuite
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.checkpoint import SimpleCheckpoint

# Create Great Expectations context
context = ge.get_context()

# COMMAND ----------

# Create Great Expectations suite
def create_ge_expectation_suite():
    """Create Great Expectations expectation suite"""
    
    suite_name = "customer_data_quality_suite"
    
    # Create expectation suite
    suite = context.create_expectation_suite(
        expectation_suite_name=suite_name,
        overwrite_existing=True
    )
    
    # Add expectations
    expectations = [
        # Completeness expectations
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "customer_id"}
        },
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "email"}
        },
        
        # Uniqueness expectations
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "customer_id"}
        },
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "email"}
        },
        
        # Format expectations
        {
            "expectation_type": "expect_column_values_to_match_regex",
            "kwargs": {
                "column": "email",
                "regex": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            }
        },
        
        # Range expectations
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "age",
                "min_value": 0,
                "max_value": 120
            }
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {
                "column": "salary",
                "min_value": 0,
                "max_value": 1000000
            }
        },
        
        # Set expectations
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "city",
                "value_set": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                             "Boston", "Seattle", "Denver", "Miami", "Atlanta", "Portland", "Las Vegas"]
            }
        }
    ]
    
    # Add expectations to suite
    for expectation in expectations:
        suite.add_expectation(ge.expectations.ExpectationConfiguration(
            expectation_type=expectation["expectation_type"],
            kwargs=expectation["kwargs"]
        ))
    
    # Save suite
    context.save_expectation_suite(suite, suite_name)
    
    print(f"Great Expectations suite '{suite_name}' created with {len(expectations)} expectations")
    return suite_name

# Create expectation suite
suite_name = create_ge_expectation_suite()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Run Great Expectations Validation

# COMMAND ----------

def run_ge_validation():
    """Run Great Expectations validation"""
    
    print("Running Great Expectations Validation...")
    
    # Convert Spark DataFrame to Pandas for GE
    df_pandas = df_sample.toPandas()
    
    # Create GE DataFrame
    df_ge = ge.from_pandas(df_pandas)
    
    # Load expectation suite
    suite = context.get_expectation_suite(suite_name)
    
    # Run validation
    results = df_ge.validate(
        expectation_suite=suite,
        result_format="SUMMARY"
    )
    
    # Display results
    print("\nValidation Results:")
    print("=" * 50)
    
    success_count = sum(1 for result in results.results if result.success)
    total_count = len(results.results)
    
    print(f"Success Rate: {success_count}/{total_count} ({success_count/total_count*100:.1f}%)")
    
    # Show failed expectations
    print("\nFailed Expectations:")
    for result in results.results:
        if not result.success:
            print(f"  ❌ {result.expectation_config.expectation_type}")
            print(f"     Column: {result.expectation_config.kwargs.get('column', 'N/A')}")
            if hasattr(result, 'result') and 'unexpected_count' in result.result:
                print(f"     Unexpected values: {result.result['unexpected_count']}")
    
    # Show passed expectations
    print("\nPassed Expectations:")
    for result in results.results:
        if result.success:
            print(f"  ✅ {result.expectation_config.expectation_type}")
    
    return results

# Run validation
ge_results = run_ge_validation()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Deequ Integration

# COMMAND ----------

# Import Deequ
from pydeequ.checks import *
from pydeequ.verification import *
from pydeequ.profiles import *
from pydeequ.suggestions import *

# Create Deequ verification suite
def create_deequ_verification():
    """Create and run Deequ verification"""
    
    print("Running Deequ Data Quality Checks...")
    
    # Define checks
    check = Check(spark, CheckLevel.Warning, "Customer Data Quality Check")
    
    # Add checks
    check = check \
        .hasSize(lambda x: x >= 10, "Dataset has at least 10 records") \
        .isComplete("customer_id") \
        .isComplete("email") \
        .isUnique("customer_id") \
        .isUnique("email") \
        .hasPattern("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$") \
        .isNonNegative("age") \
        .isNonNegative("salary") \
        .isContainedIn("city", ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
                               "Boston", "Seattle", "Denver", "Miami", "Atlanta", "Portland", "Las Vegas"]) \
        .hasMin("age", lambda x: x >= 0) \
        .hasMax("age", lambda x: x <= 120) \
        .hasMin("salary", lambda x: x >= 0) \
        .hasMax("salary", lambda x: x <= 1000000)
    
    # Run verification
    verification_result = VerificationSuite(spark).onData(df_sample).addCheck(check).run()
    
    # Display results
    print("\nDeequ Verification Results:")
    print("=" * 50)
    
    # Convert to DataFrame for better display
    result_df = VerificationResult.checkResultsAsDataFrame(spark, verification_result)
    result_df.show(truncate=False)
    
    # Summary
    total_checks = result_df.count()
    passed_checks = result_df.filter(col("constraint_status") == "Success").count()
    
    print(f"\nSummary: {passed_checks}/{total_checks} checks passed ({passed_checks/total_checks*100:.1f}%)")
    
    return verification_result

# Run Deequ verification
deequ_results = create_deequ_verification()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Data Profiling with Deequ

# COMMAND ----------

def run_data_profiling():
    """Run data profiling with Deequ"""
    
    print("Running Data Profiling...")
    
    # Create profiler
    profiler = ColumnProfilerRunner(spark).onData(df_sample)
    
    # Run profiling
    profile_result = profiler.run()
    
    # Display profile results
    print("Data Profile Results:")
    print("=" * 50)
    
    # Convert to DataFrame
    profile_df = ProfileResult.profilesAsDataFrame(spark, profile_result)
    profile_df.show(truncate=False)
    
    # Show statistics
    print("\nColumn Statistics:")
    for row in profile_df.collect():
        column_name = row["column"]
        data_type = row["dataType"]
        completeness = row["completeness"]
        print(f"  {column_name} ({data_type}): {completeness:.1%} complete")
        
        # Show additional stats if available
        if "mean" in row and row["mean"] is not None:
            print(f"    Mean: {row['mean']:.2f}")
        if "stddev" in row and row["stddev"] is not None:
            print(f"    Std Dev: {row['stddev']:.2f}")
        if "min" in row and row["min"] is not None:
            print(f"    Min: {row['min']}")
        if "max" in row and row["max"] is not None:
            print(f"    Max: {row['max']}")
    
    return profile_result

# Run data profiling
profile_results = run_data_profiling()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Anomaly Detection

# COMMAND ----------

def run_anomaly_detection():
    """Run anomaly detection"""
    
    print("Running Anomaly Detection...")
    
    # Create anomaly detection checks
    check = Check(spark, CheckLevel.Error, "Anomaly Detection")
    
    # Add anomaly detection rules
    check = check \
        .hasAnomalyDetection("age", AnomalyCheckConfig(
            metric=AnomalyCheckConfig.Metric.Mean,
            strategy=AnomalyCheckConfig.Strategy.AbsoluteChange,
            threshold=10.0,
            filter_condition="age > 0"  # Exclude invalid ages
        )) \
        .hasAnomalyDetection("salary", AnomalyCheckConfig(
            metric=AnomalyCheckConfig.Metric.Mean,
            strategy=AnomalyCheckConfig.Strategy.AbsoluteChange,
            threshold=10000.0,
            filter_condition="salary > 0"  # Exclude negative salaries
        ))
    
    # Run anomaly detection
    anomaly_result = VerificationSuite(spark).onData(df_sample).addCheck(check).run()
    
    # Display results
    print("\nAnomaly Detection Results:")
    print("=" * 50)
    
    anomaly_df = VerificationResult.checkResultsAsDataFrame(spark, anomaly_result)
    anomaly_df.show(truncate=False)
    
    return anomaly_result

# Run anomaly detection
anomaly_results = run_anomaly_detection()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Quality Monitoring Dashboard

# COMMAND ----------

def create_quality_dashboard():
    """Create quality monitoring dashboard"""
    
    print("Data Quality Monitoring Dashboard")
    print("=" * 60)
    
    # 1. Overall quality score
    print("1. Overall Quality Score:")
    
    # Calculate quality metrics
    total_records = df_sample.count()
    valid_records = df_sample.filter(
        col("customer_id").isNotNull() &
        col("email").isNotNull() &
        col("age") > 0 &
        col("age") <= 120 &
        col("salary") > 0 &
        col("city").isNotNull()
    ).count()
    
    quality_score = (valid_records / total_records) * 100
    
    print(f"   Total Records: {total_records}")
    print(f"   Valid Records: {valid_records}")
    print(f"   Quality Score: {quality_score:.1f}%")
    
    # 2. Quality issues breakdown
    print("\n2. Quality Issues Breakdown:")
    
    issues = {
        "Null Customer ID": df_sample.filter(col("customer_id").isNull()).count(),
        "Null Email": df_sample.filter(col("email").isNull()).count(),
        "Invalid Email Format": df_sample.filter(~col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")).count(),
        "Invalid Age": df_sample.filter((col("age") <= 0) | (col("age") > 120)).count(),
        "Invalid Salary": df_sample.filter(col("salary") <= 0).count(),
        "Null City": df_sample.filter(col("city").isNull()).count(),
    }
    
    for issue, count in issues.items():
        if count > 0:
            print(f"   ❌ {issue}: {count} records")
    
    # 3. Data completeness
    print("\n3. Data Completeness:")
    for column in df_sample.columns:
        null_count = df_sample.filter(col(column).isNull()).count()
        completeness = ((total_records - null_count) / total_records) * 100
        print(f"   {column}: {completeness:.1f}% complete")
    
    # 4. Data distribution
    print("\n4. Data Distribution:")
    
    # Age distribution
    print("   Age Distribution:")
    age_stats = df_sample.filter(col("age") > 0).select(
        min("age").alias("min_age"),
        max("age").alias("max_age"),
        avg("age").alias("avg_age")
    ).collect()[0]
    print(f"     Min: {age_stats['min_age']}, Max: {age_stats['max_age']}, Avg: {age_stats['avg_age']:.1f}")
    
    # Salary distribution
    print("   Salary Distribution:")
    salary_stats = df_sample.filter(col("salary") > 0).select(
        min("salary").alias("min_salary"),
        max("salary").alias("max_salary"),
        avg("salary").alias("avg_salary")
    ).collect()[0]
    print(f"     Min: ${salary_stats['min_salary']:,.0f}, Max: ${salary_stats['max_salary']:,.0f}, Avg: ${salary_stats['avg_salary']:,.0f}")
    
    # 5. Recommendations
    print("\n5. Quality Improvement Recommendations:")
    print("   🔧 Fix email format validation")
    print("   🔧 Implement age range validation")
    print("   🔧 Add salary minimum threshold")
    print("   🔧 Handle missing city data")
    print("   🔧 Implement duplicate detection")
    print("   🔧 Add data quality gates to pipeline")

# Create quality dashboard
create_quality_dashboard()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Quality Gates for Data Pipeline

# COMMAND ----------

def implement_quality_gates():
    """Implement quality gates for data pipeline"""
    
    print("Implementing Quality Gates...")
    
    # Define quality thresholds
    quality_thresholds = {
        "min_quality_score": 80.0,  # Minimum 80% quality score
        "max_null_percentage": 10.0,  # Maximum 10% null values per column
        "max_duplicate_percentage": 5.0,  # Maximum 5% duplicate records
        "required_columns": ["customer_id", "email", "age", "salary", "city"]
    }
    
    # Quality gate checks
    def check_quality_gates(df):
        """Check if data passes quality gates"""
        
        results = {}
        
        # 1. Check required columns
        missing_columns = set(quality_thresholds["required_columns"]) - set(df.columns)
        results["missing_columns"] = len(missing_columns) == 0
        
        # 2. Check data completeness
        total_records = df.count()
        completeness_checks = {}
        
        for column in quality_thresholds["required_columns"]:
            if column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_percentage = (null_count / total_records) * 100
                completeness_checks[column] = null_percentage <= quality_thresholds["max_null_percentage"]
        
        results["completeness"] = all(completeness_checks.values())
        
        # 3. Check for duplicates
        if "customer_id" in df.columns:
            duplicate_count = df.count() - df.select("customer_id").distinct().count()
            duplicate_percentage = (duplicate_count / total_records) * 100
            results["duplicates"] = duplicate_percentage <= quality_thresholds["max_duplicate_percentage"]
        else:
            results["duplicates"] = False
        
        # 4. Overall quality score
        valid_records = df.filter(
            col("customer_id").isNotNull() &
            col("email").isNotNull() &
            col("age") > 0 &
            col("age") <= 120 &
            col("salary") > 0 &
            col("city").isNotNull()
        ).count()
        
        quality_score = (valid_records / total_records) * 100
        results["quality_score"] = quality_score >= quality_thresholds["min_quality_score"]
        
        return results, quality_score
    
    # Run quality gate checks
    gate_results, quality_score = check_quality_gates(df_sample)
    
    print("Quality Gate Results:")
    print("=" * 40)
    print(f"Required Columns Present: {'✅' if gate_results['missing_columns'] else '❌'}")
    print(f"Data Completeness: {'✅' if gate_results['completeness'] else '❌'}")
    print(f"Duplicate Check: {'✅' if gate_results['duplicates'] else '❌'}")
    print(f"Quality Score ({quality_score:.1f}%): {'✅' if gate_results['quality_score'] else '❌'}")
    
    # Overall gate result
    all_passed = all(gate_results.values())
    print(f"\nOverall Quality Gate: {'✅ PASSED' if all_passed else '❌ FAILED'}")
    
    if not all_passed:
        print("\nQuality Gate Failed - Data Pipeline Should Stop!")
        print("Recommended Actions:")
        print("  🔧 Fix data quality issues before proceeding")
        print("  🔧 Review data source and transformation logic")
        print("  🔧 Implement data cleansing steps")
    else:
        print("\nQuality Gate Passed - Data Pipeline Can Continue!")
    
    return gate_results, quality_score

# Implement quality gates
gate_results, quality_score = implement_quality_gates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Automated Quality Monitoring

# COMMAND ----------

def create_automated_monitoring():
    """Create automated quality monitoring system"""
    
    print("Setting up Automated Quality Monitoring...")
    
    # Create quality metrics table
    quality_metrics_schema = StructType([
        StructField("check_timestamp", TimestampType(), True),
        StructField("table_name", StringType(), True),
        StructField("quality_score", DoubleType(), True),
        StructField("total_records", IntegerType(), True),
        StructField("valid_records", IntegerType(), True),
        StructField("null_customer_id", IntegerType(), True),
        StructField("null_email", IntegerType(), True),
        StructField("invalid_age", IntegerType(), True),
        StructField("invalid_salary", IntegerType(), True),
        StructField("duplicate_records", IntegerType(), True),
        StructField("gate_passed", BooleanType(), True)
    ])
    
    # Create quality metrics table
    quality_metrics_path = f"{curated_path}quality/quality_metrics"
    
    # Sample metrics data
    current_time = datetime.now()
    total_records = df_sample.count()
    valid_records = df_sample.filter(
        col("customer_id").isNotNull() &
        col("email").isNotNull() &
        col("age") > 0 &
        col("age") <= 120 &
        col("salary") > 0 &
        col("city").isNotNull()
    ).count()
    
    metrics_data = [(
        current_time,
        "customer_data_quality_test",
        quality_score,
        total_records,
        valid_records,
        df_sample.filter(col("customer_id").isNull()).count(),
        df_sample.filter(col("email").isNull()).count(),
        df_sample.filter((col("age") <= 0) | (col("age") > 120)).count(),
        df_sample.filter(col("salary") <= 0).count(),
        total_records - df_sample.select("customer_id").distinct().count(),
        all(gate_results.values())
    )]
    
    metrics_df = spark.createDataFrame(metrics_data, quality_metrics_schema)
    metrics_df.write.format("delta").mode("append").save(quality_metrics_path)
    
    # Create table
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS quality_metrics
    USING DELTA
    LOCATION '{quality_metrics_path}'
    """)
    
    print("Quality metrics table created and populated")
    
    # Create monitoring queries
    print("\nQuality Monitoring Queries:")
    print("=" * 50)
    
    # Recent quality trends
    print("1. Recent Quality Trends:")
    trend_query = """
    SELECT 
        check_timestamp,
        table_name,
        quality_score,
        total_records,
        gate_passed
    FROM quality_metrics
    ORDER BY check_timestamp DESC
    LIMIT 10
    """
    spark.sql(trend_query).show()
    
    # Quality alerts
    print("\n2. Quality Alerts:")
    alert_query = """
    SELECT 
        check_timestamp,
        table_name,
        quality_score,
        CASE 
            WHEN quality_score < 80 THEN 'CRITICAL'
            WHEN quality_score < 90 THEN 'WARNING'
            ELSE 'OK'
        END as alert_level
    FROM quality_metrics
    WHERE check_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
    ORDER BY check_timestamp DESC
    """
    spark.sql(alert_query).show()
    
    return quality_metrics_path

# Create automated monitoring
monitoring_path = create_automated_monitoring()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Quality Integration with Delta Lake

# COMMAND ----------

def integrate_quality_with_delta():
    """Integrate quality checks with Delta Lake workflows"""
    
    print("Integrating Quality Checks with Delta Lake...")
    
    # Create quality-enabled table
    quality_table_path = f"{curated_path}quality/customer_data_with_quality"
    
    # Add quality metadata to data
    df_with_quality = df_sample.withColumn("quality_score", lit(quality_score)) \
                              .withColumn("quality_timestamp", current_timestamp()) \
                              .withColumn("quality_gate_passed", lit(all(gate_results.values()))) \
                              .withColumn("data_source", lit("quality_test_pipeline"))
    
    # Write to Delta with quality metadata
    df_with_quality.write \
        .format("delta") \
        .mode("overwrite") \
        .save(quality_table_path)
    
    # Create table
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS customer_data_with_quality
    USING DELTA
    LOCATION '{quality_table_path}'
    """)
    
    print("Quality-enabled Delta table created")
    
    # Create quality validation function
    def validate_data_quality(table_path, min_quality_score=80.0):
        """Validate data quality and return results"""
        
        df = spark.read.format("delta").load(table_path)
        
        # Run quality checks
        total_records = df.count()
        valid_records = df.filter(
            col("customer_id").isNotNull() &
            col("email").isNotNull() &
            col("age") > 0 &
            col("age") <= 120 &
            col("salary") > 0 &
            col("city").isNotNull()
        ).count()
        
        current_quality_score = (valid_records / total_records) * 100
        
        # Update quality metadata
        df_updated = df.withColumn("quality_score", lit(current_quality_score)) \
                      .withColumn("quality_timestamp", current_timestamp()) \
                      .withColumn("quality_gate_passed", lit(current_quality_score >= min_quality_score))
        
        # Write updated data
        df_updated.write \
            .format("delta") \
            .mode("overwrite") \
            .save(table_path)
        
        return current_quality_score >= min_quality_score, current_quality_score
    
    # Test quality validation
    print("\nTesting Quality Validation:")
    quality_passed, current_score = validate_data_quality(quality_table_path)
    print(f"Quality Gate Passed: {'✅' if quality_passed else '❌'}")
    print(f"Current Quality Score: {current_score:.1f}%")
    
    # Show quality-enabled data
    print("\nQuality-Enabled Data Sample:")
    spark.sql("SELECT * FROM customer_data_with_quality LIMIT 5").show()
    
    return quality_table_path

# Integrate quality with Delta
quality_integrated_path = integrate_quality_with_delta()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC This quality integration demonstration showed:
# MAGIC 
# MAGIC ### **Quality Tools Implemented:**
# MAGIC 1. ✅ **Great Expectations**: Comprehensive data validation framework
# MAGIC 2. ✅ **Deequ**: Spark-native data quality checks and profiling
# MAGIC 3. ✅ **Anomaly Detection**: Automated detection of data anomalies
# MAGIC 4. ✅ **Quality Gates**: Automated quality thresholds and validation
# MAGIC 5. ✅ **Monitoring Dashboard**: Real-time quality monitoring and alerting
# MAGIC 6. ✅ **Delta Lake Integration**: Quality metadata embedded in data
# MAGIC 
# MAGIC ### **Quality Checks Implemented:**
# MAGIC - **Completeness**: Null value detection and validation
# MAGIC - **Uniqueness**: Duplicate record detection
# MAGIC - **Format Validation**: Email format and data type validation
# MAGIC - **Range Validation**: Age and salary range checks
# MAGIC - **Referential Integrity**: City validation against allowed values
# MAGIC - **Anomaly Detection**: Statistical anomaly detection
# MAGIC 
# MAGIC ### **Quality Metrics Tracked:**
# MAGIC - **Quality Score**: Overall data quality percentage
# MAGIC - **Completeness**: Column-level null value tracking
# MAGIC - **Validity**: Format and range validation results
# MAGIC - **Uniqueness**: Duplicate record detection
# MAGIC - **Gate Status**: Pass/fail status for quality gates
# MAGIC 
# MAGIC ### **Integration Benefits:**
# MAGIC - **Automated Validation**: Quality checks run automatically
# MAGIC - **Pipeline Integration**: Quality gates prevent bad data
# MAGIC - **Historical Tracking**: Quality metrics over time
# MAGIC - **Alerting**: Automated alerts for quality issues
# MAGIC - **Audit Trail**: Complete quality history and metadata
# MAGIC 
# MAGIC ### **Storage Locations:**
# MAGIC - **Test Data**: `{quality_test_path}`
# MAGIC - **Quality Metrics**: `{monitoring_path}`
# MAGIC - **Quality-Enabled Data**: `{quality_integrated_path}`
# MAGIC 
# MAGIC The quality framework is now integrated with your data pipeline and ready for production use!
