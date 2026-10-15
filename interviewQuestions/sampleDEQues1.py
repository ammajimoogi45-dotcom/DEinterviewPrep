# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
from pyspark.sql.types import *
import datetime
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,create a dataframe

schema = StructType([
    StructField('id',IntegerType(),True),
    StructField('Name',StringType(),True),
    StructField('ArrayCol',ArrayType(StringType()),True),
    StructField('Diccol',MapType(StringType(),StringType()),True),
    StructField('structType',
                StructType([StructField('arr',ArrayType(StringType()),True),
                             StructField('val',IntegerType(),True)]), True)
])

sampleDF = spark.createDataFrame([(1,'ammaji',[1,2,3,4],{'1':'ammaji','2':'sample','3':'sample2','4':'sample3'},([1,2,3,4],1)),],schema)
display(sampleDF)

# COMMAND ----------

# DBTITLE 1,Avro fileFormats
#avro - commonly used for streaming data , stores data row by row , it stores schema too
schema = StructType([
    StructField("orderId", LongType(), False),
    StructField("orderDate", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("priority", IntegerType(), True),

    # Nested customer struct
    StructField("customer", StructType([
        StructField("customerId", StringType(), False),
        StructField("name", StructType([
            StructField("firstName", StringType(), True),
            StructField("middleName", StringType(), True),
            StructField("lastName", StringType(), True)
        ]), True),
        StructField("email", StringType(), True),
        StructField("isPremium", BooleanType(), True),

        # Array of structs
        StructField("phoneNumbers", ArrayType(
            StructType([
                StructField("type", StringType(), True),
                StructField("number", StringType(), True),
                StructField("verified", BooleanType(), True)
            ])
        ), True),

        # Nested address struct
        StructField("address", StructType([
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("postalCode", StringType(), True),
            StructField("coordinates", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True)
            ]), True)
        ]), True)
    ]), True),

    # Array of product structs
    StructField("items", ArrayType(
        StructType([
            StructField("productId", StringType(), True),
            StructField("productName", StringType(), True),
            StructField("category", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unitPrice", DoubleType(), True),
            StructField("discount", DoubleType(), True),
            StructField("tags", ArrayType(StringType()), True),

            # Dynamic product attributes
            StructField("attributes", MapType(
                StringType(), StringType()
            ), True),

            # Array nested inside an array element struct
            StructField("reviews", ArrayType(
                StructType([
                    StructField("reviewId", StringType(), True),
                    StructField("rating", IntegerType(), True),
                    StructField("comment", StringType(), True)
                ])
            ), True)
        ])
    ), True),

    # Payment struct
    StructField("payment", StructType([
        StructField("method", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("totalAmount", DoubleType(), True),
        StructField("paid", BooleanType(), True),
        StructField("attempts", ArrayType(
            StructType([
                StructField("attemptNo", IntegerType(), True),
                StructField("paymentStatus", StringType(), True),
                StructField("transactionId", StringType(), True)
            ])
        ), True)
    ]), True),

    # Shipping struct
    StructField("shipping", StructType([
        StructField("carrier", StringType(), True),
        StructField("trackingNumber", StringType(), True),
        StructField("estimatedDelivery", TimestampType(), True),
        StructField("events", ArrayType(
            StructType([
                StructField("eventTime", TimestampType(), True),
                StructField("location", StringType(), True),
                StructField("eventType", StringType(), True)
            ])
        ), True)
    ]), True),

    # Map and nullable field
    StructField("metadata", MapType(
        StringType(), StringType()
    ), True),

    StructField("giftMessage", StringType(), True),
    ## added in second overwrite
    StructField("additionalMessage", StringType(), True),
    StructField("addtionalcomment",StringType(),True)
])
data = [
    {
        "orderId": 1001,
        "orderDate": datetime.datetime(2026, 10, 10, 10, 30),
        "status": "SHIPPED",
        "priority": 1,

        "customer": {
            "customerId": "C101",
            "name": {
                "firstName": "Ammaji",
                "middleName": None,
                "lastName": "Moogi"
            },
            "email": "ammaji@example.com",
            "isPremium": True,
            "phoneNumbers": [
                {
                    "type": "mobile",
                    "number": "9000000001",
                    "verified": True
                },
                {
                    "type": "work",
                    "number": "9000000002",
                    "verified": False
                }
            ],
            "address": {
                "street": "12 Lake Road",
                "city": "Bengaluru",
                "state": "Karnataka",
                "postalCode": "560001",
                "coordinates": {
                    "latitude": 12.9716,
                    "longitude": 77.5946
                }
            }
        },

        "items": [
            {
                "productId": "P101",
                "productName": "Laptop",
                "category": "Electronics",
                "quantity": 1,
                "unitPrice": 75000.0,
                "discount": 5000.0,
                "tags": ["computer", "work", "premium"],
                "attributes": {
                    "brand": "BrandA",
                    "ram": "16GB",
                    "color": "Silver"
                },
                "reviews": [
                    {
                        "reviewId": "R1",
                        "rating": 5,
                        "comment": "Excellent"
                    },
                    {
                        "reviewId": "R2",
                        "rating": 4,
                        "comment": "Good performance"
                    }
                ]
            },
            {
                "productId": "P102",
                "productName": "Mouse",
                "category": "Accessories",
                "quantity": 2,
                "unitPrice": 800.0,
                "discount": 50.0,
                "tags": ["wireless", "accessory"],
                "attributes": {
                    "brand": "BrandB",
                    "connectivity": "Bluetooth"
                },
                "reviews": []
            }
        ],

        "payment": {
            "method": "UPI",
            "currency": "INR",
            "totalAmount": 76550.0,
            "paid": True,
            "attempts": [
                {
                    "attemptNo": 1,
                    "paymentStatus": "FAILED",
                    "transactionId": "TXN100"
                },
                {
                    "attemptNo": 2,
                    "paymentStatus": "SUCCESS",
                    "transactionId": "TXN101"
                }
            ]
        },

        "shipping": {
            "carrier": "FastExpress",
            "trackingNumber": "TRK1001",
            "estimatedDelivery": datetime.datetime(2026, 10, 15, 18, 0),
            "events": [
                {
                    "eventTime": datetime.datetime(2026, 10, 11, 9, 0),
                    "location": "Bengaluru",
                    "eventType": "PICKED_UP"
                },
                {
                    "eventTime": datetime.datetime(2026, 10, 12, 14, 0),
                    "location": "Chennai",
                    "eventType": "IN_TRANSIT"
                }
            ]
        },

        "metadata": {
            "source": "mobile_app",
            "campaign": "festival_sale",
            "device": "Android"
        },
        "giftMessage": None
    },

    {
        "orderId": 1002,
        "orderDate": datetime.datetime(2026, 10, 11, 14, 15),
        "status": "PROCESSING",
        "priority": 3,

        "customer": {
            "customerId": "C102",
            "name": {
                "firstName": "Ravi",
                "middleName": "Kumar",
                "lastName": "Rao"
            },
            "email": None,
            "isPremium": False,
            "phoneNumbers": [],
            "address": {
                "street": "5 Market Street",
                "city": "Hyderabad",
                "state": "Telangana",
                "postalCode": "500001",
                "coordinates": None
            }
        },

        "items": [
            {
                "productId": "P201",
                "productName": "Headphones",
                "category": "Electronics",
                "quantity": 1,
                "unitPrice": 2500.0,
                "discount": 0.0,
                "tags": ["audio"],
                "attributes": {
                    "brand": "BrandC",
                    "type": "Over-ear"
                },
                "reviews": None
            }
        ],

        "payment": {
            "method": "CARD",
            "currency": "INR",
            "totalAmount": 2500.0,
            "paid": False,
            "attempts": [
                {
                    "attemptNo": 1,
                    "paymentStatus": "PENDING",
                    "transactionId": None
                }
            ]
        },

        "shipping": None,

        "metadata": {
            "source": "website",
            "device": "Desktop"
        },
        "giftMessage": "Happy Birthday!",
        # added in second run
        "additionalMessage": "yeahFinaly"
    },

    {
        "orderId": 1003,
        "orderDate": datetime.datetime(2026, 10, 12, 8, 0),
        "status": "CANCELLED",
        "priority": None,
        "customer": None,
        "items": [],
        "payment": None,
        "shipping": None,
        "metadata": None,
        "giftMessage": None
    }
]
avroDF=spark.createDataFrame(data,schema).withColumn(
    "orderMonday" ,lit('yes')
)
avroDF.write.format('avro').mode("overwrite").save('/Volumes/learning/default/filesystem/order.avro')
display(spark.read.format("avro").load('/Volumes/learning/default/filesystem/order.avro')
        .select(col('customer.customerId'),col('customer.name.*')
        ))


# COMMAND ----------

# DBTITLE 1,CSV fileformat
#"," seperated text file.
spark.read.format('csv').option(
    "header",True # First row contains column names
).option(
    "inferSchema",True # Infers data types from values
).option(
    "multiLine", True #Allows quoted fields to span multiple physical lines
).option(
   "sep", "," #Defines the delimiter
).option(
    "quote",'"'
).option("escape", "\\")
.load(
    filepath
)


display(spark.read.format('csv').option('header',True).load(
"/Volumes/learning/default/filesystem/invalidAge.csv"))
ageSchema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
display(spark.read.format('csv').option('header',True).option("mode","PERMISSIVE").schema(ageSchema).load('/Volumes/learning/default/filesystem/invalidAge.csv'))
display(spark.read.format('csv').option('header',True).option("mode","DROPMALFORMED").schema(ageSchema).load('/Volumes/learning/default/filesystem/invalidAge.csv'))
display(spark.read.format('csv').option('header',True).option("mode","FAILFAST").schema(ageSchema).load('/Volumes/learning/default/filesystem/invalidAge.csv'))

# COMMAND ----------

# MAGIC %fs ls '/Volumes/learning/default/filesystem/'

# COMMAND ----------

# DBTITLE 1,excel read package
!pip install openpyxl

# COMMAND ----------

# DBTITLE 1,excel file
## excel reading 
import openpyxl
from openpyxl import Workbook
from io import BytesIO

# Read the Excel file as binary
binary_df = spark.read.format("binaryFile").load(
    "/Volumes/learning/default/filesystem/HumanResources.Department.xlsx"
)

content = binary_df.collect()[0]["content"]

# Parse with openpyxl
wb = openpyxl.load_workbook(BytesIO(content), read_only=True, data_only=True)
print(wb.sheetnames)
ws = wb.worksheets[0]  # 0-based index; change to 1 for second sheet, etc.
rows = list(ws.iter_rows(values_only=True))
wb.close()
print(rows)

# Create Spark DataFrame with header row as column names
headers = [str(c).strip() if c else f"_c{i}" for i, c in enumerate(rows[0])]
print(headers)
# headers_ts= [i for i in rows[0]]
# print(headers_ts)
data = rows[1:]

df = spark.createDataFrame(data, schema=headers)
display(df)


## excel writing 


wb = Workbook()
ws = wb.active
print(list(enumerate(df.columns, 1)))
# Write headers
for col_idx, col_name in enumerate(df.columns, 1):
    ws.cell(row=1, column=col_idx, value=col_name)

# Write data rows
for row_idx, row in enumerate(df.collect(), 2):
    for col_idx, value in enumerate(row, 1):
        ws.cell(row=row_idx, column=col_idx, value=value)

# Save to in-memory buffer, then write to UC volume
buffer = BytesIO()
wb.save(buffer)
wb.close()

print(rows[0])
with open("/Volumes/learning/default/filesystem/Department.xlsx", "wb") as f:
    f.write(buffer.getvalue())

# COMMAND ----------

# DBTITLE 1,OCR fileformat
df.write.format('orc').save('/Volumes/learning/default/filesystem/Department')
display(spark.read.format('orc').load('/Volumes/learning/default/filesystem/Department'))
#columar format.
# it is more compressed as compare to parquet .
# it used mostly in hadoop ecosystem

# COMMAND ----------

# DBTITLE 1,Parquet fileformat
spark.read.format('parquet').load('filepath')
spark.write.format('parquet').mode('overwrite').save('filepath')

#columar format
#filter out base on column is easy
#selecting base on column is easy
#schema is stored in metadata
#merge,update,delete won't support