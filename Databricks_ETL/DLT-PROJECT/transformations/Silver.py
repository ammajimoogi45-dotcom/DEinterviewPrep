from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

catalogName= spark.conf.get('catalogName')
Schema =spark.conf.get('Schema')
volume=f"/Volumes/{catalogName}/{Schema}/rawfilesystem/Landing/"

def exceptionCondition(
    table_name
):
    if table_name =='bronze_order':
       return dp.expect_all_or_drop({
            'valuetocheck': 'order_id is not null',
            'orderstatuscheck': "order_status != 'cancelled'"
       })
    elif table_name =='bronze_payment':
       return dp.expect_or_drop('paymentCheckValue','payment_value > 100')
    elif table_name =='bronze_order_reviews':
       return dp.expect('reviewscoreCheck','review_score > 3')
    elif table_name =='bronze_order_items':
        return dp.expect_or_drop('priceCheck','price > 100')

table_names=['bronze_order','bronze_order_items','bronze_order_reviews','bronze_payment']
silverTableNames= ['silver_order','silver_order_items','silver_order_reviews','silver_payment']
for tableName,silverTableName in zip(table_names,silverTableNames):
        @dp.table(name=silverTableName)
        @exceptionCondition(tableName)
        def silverOrder(tableName=tableName):
            if tableName =='bronze_order_reviews':
                return spark.readStream.table(tableName).filter(
                    col('_rescued_data').isNull()
                ).dropDuplicates().drop('_rescued_data')
            else:
                return spark.readStream.table(tableName).filter(
                    col('_rescued_data').isNull()
                ).dropDuplicates().drop('_rescued_data')
 

dp.create_streaming_table(name='silver_order_cdc')
dp.create_auto_cdc_flow(
            target='silver_order_cdc',
            source='silver_order',
            keys= ['customer_id'],
            sequence_by= 'stg_upload_date',
            apply_as_deletes = None, # e.g. "op = 'D'" (skip: you have no op column)
            apply_as_truncates = None, # SCD Type 1 only
            column_list= None, #keep only these columns
            except_column_list=None, # or leave these columns out
            stored_as_scd_type=1,    #1 = latest only, 2 = history
            track_history_column_list=None, ## SCD 2: only these columns create new versions
            track_history_except_column_list=None, # 
            ignore_null_updates=None, ## # partial rows (see earlier)
            columns_to_update=None, #partial rows via an ARRAY<STRING> column
            name='order_cdc',
            once=False

        )
 