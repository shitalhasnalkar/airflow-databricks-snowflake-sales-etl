print("Databricks Sales ETL Project")

df = spark.range(10)

display(df)


import requests

url = "https://ethanssalesdemo.blob.core.windows.net/"

try:
    response = requests.get(url, timeout=15)
    print("HTTP status:", response.status_code)
except Exception as e:
    print("Connection test failed:", str(e))


import requests
from urllib.parse import urlsplit, urlunsplit

# Paste your new container SAS URL here locally.
container_sas_url = "https://ethanssalesdemo.blob.core.windows.net/sales?sp=racwl&st=2026-10-17T06:13:55Z&se=2026-10-17T14:28:55Z&spr=https&sv=2026-02-06&sr=c&sig=2EGGTYTjEQ5fjew%2B%2FzZCpHyV82%2F4PuwDDLdbbNx1JLY%3D"

# Build the URL for the actual input file.
parts = urlsplit(container_sas_url)

file_url = urlunsplit((
    parts.scheme,
    parts.netloc,
    parts.path.rstrip("/") + "/input/sales.csv",
    parts.query,
    ""
))

response = requests.get(file_url, timeout=30)

print("HTTP status:", response.status_code)

if response.status_code == 200:
    print("SUCCESS: Databricks can read sales.csv")
    print("File size:", len(response.content), "bytes")
else:
    print("Request failed.")
    print(response.text[:500])



import pandas as pd
from io import BytesIO

# Download the CSV using the SAS URL
response = requests.get(file_url, timeout=30)
response.raise_for_status()

# Read the CSV into a Spark DataFrame
pdf = pd.read_csv(BytesIO(response.content))

df = spark.createDataFrame(pdf)

display(df)

df.printSchema()

print("Total rows:", df.count())

from pyspark.sql.functions import col, when, round

df_transformed = (
    df
    .withColumn("quantity", col("quantity").cast("integer"))
    .withColumn("price", col("price").cast("double"))
    .withColumn(
        "amount",
        round(col("quantity") * col("price"), 2)
    )
    .withColumn(
        "discount",
        when(col("quantity") * col("price") >= 100000,
             round(col("quantity") * col("price") * 0.10, 2))
        .when(col("quantity") * col("price") >= 50000,
              round(col("quantity") * col("price") * 0.05, 2))
        .otherwise(0.0)
    )
    .withColumn(
        "net_amount",
        round(col("amount") - col("discount"), 2)
    )
)

display(df_transformed)


from urllib.parse import urlsplit, urlunsplit
import csv
from io import StringIO

# Convert the transformed Spark DataFrame to CSV text.
# Suitable for this small training dataset.
pdf_result = df_transformed.toPandas()

csv_buffer = StringIO()
pdf_result.to_csv(csv_buffer, index=False)

csv_bytes = csv_buffer.getvalue().encode("utf-8")

# Build the output blob URL.
parts = urlsplit(container_sas_url)

output_url = urlunsplit((
    parts.scheme,
    parts.netloc,
    parts.path.rstrip("/") + "/output/sales_result.csv",
    parts.query,
    ""
))

headers = {
    "x-ms-blob-type": "BlockBlob",
    "x-ms-version": "2023-11-03",
    "Content-Type": "text/csv"
}

upload_response = requests.put(
    output_url,
    headers=headers,
    data=csv_bytes,
    timeout=60
)

print("Upload HTTP status:", upload_response.status_code)

upload_response.raise_for_status()

print("SUCCESS: Transformed CSV uploaded to Blob Storage")
print("Rows written:", len(pdf_result))



