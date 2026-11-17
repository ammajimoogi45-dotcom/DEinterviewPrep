from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp
catalogName= spark.conf.get('catalogName')
Schema =spark.conf.get('Schema')
volume=f"/Volumes/{catalogName}/{Schema}/rawfilesystem/Landing/"

tableNames=['bronze_order','bronze_order_items','bronze_order_reviews','bronze_payment']
for tableName in tableNames:
    @dp.table(name=tableName)
    def loadbronze(tableName=tableName):
        fileName= tableName.replace('bronze_','')
        if fileName == 'payment':
            fileName='paymentDF'
        return spark.readStream.format('cloudFiles').option(
            'cloudFiles.format',"csv"
        ).option(
            "header",True
        ).option(
            'cloudFiles.inferColumnTypes',True
        ).option(
            'pathGlobFilter','*.csv'
        ).load(volume+fileName+"/").withColumn('Stg_upload_date', current_timestamp())

#adding a append flow
# @dp.append_flow(target='bronze_order')
# def secondSource():
#     return spark.readStream.format('CloudFiles').option(
#         'cloudFiles.format',"csv"
#     ).option(
#         'header',True
#     ).option(
#         'cloudFiles.inferColumnTypes', True
#     ).option(
#         'pathGlobFilter', '*.csv'
#     ).load(
#         f"{volume}/order_sample"
#     )

@dp.table(name='organisation')
def organisation(table='bronze_organisation'):
    return spark.readStream.format('cloudFiles').option(
        'cloudFiles.format','json'
    ).option(
        'pathGlobFilter','*.json'
    ).load(f"{volume}/json")



