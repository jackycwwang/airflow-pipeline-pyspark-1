from pyspark.sql import SparkSession

def process_data():

    spark = SparkSession.builder.appName("Joining Datasets").getOrCreate()

    # Configure your GCP bucket and paths
    bucket = "landing-zone-1"
    emp_data_path = f"gs://{bucket}/employee.csv"
    dept_data_path = f"gs://{bucket}/department.csv"
    output_path = f"gs://{bucket}/joined_output"

    # Read datasets
    employee = spark.read.csv(emp_data_path, header=True, inferSchema=True)
    department = spark.read.csv(dept_data_path, header=True, inferSchema=True)

    # Filter employee data
    filtered_emp = employee.filter(employee.salary > 50000)

    # Join both datasets
    joined_data = filtered_emp.join(department, "dept_id", "inner")

    # Write output
    joined_data.write.csv(output_path, header=True)

    spark.stop()

if __name__ == "__main__":
    process_data()