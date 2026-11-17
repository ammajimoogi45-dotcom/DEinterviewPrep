from pyspark import pipelines  as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dp.materialized_view(name='FinalCustorderMV')
def Finalcustorder():
    return (
        spark.read.table('silver_order').filter(
        col('order_status')=='delivered'
    ).alias('A').join(
        spark.read.table('silver_order_items').alias('B'),col('A.order_id')==col('B.order_id')
    ).join(
        spark.read.table('silver_order_reviews').alias('c'), col('B.order_id') == col('c.order_id'),'inner'
    ).join(
        spark.read.table('silver_payment').alias('D'), col('c.order_id')==col('D.order_id'),'inner'
    ).select(
          'A.order_id',
          'A.order_purchase_timestamp',
          'A.order_delivered_customer_date',
          'A.customer_id',
          'A.order_status',
          'c.review_comment_message',
          'c.review_id',
          'c.review_creation_date',
          'B.price',
          'B.product_id',
          'B.seller_id',
          'D.payment_sequential',
          'D.payment_type',
          'D.payment_installments',
          'D.payment_value'

    )
        )

        
    




    