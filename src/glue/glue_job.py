import sys
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp

# Inicialização do Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Parâmetros
input_database = "raw_db"
output_path = 's3://pncprt/gold/'

glue_client = boto3.client('glue')
tables = glue_client.get_tables(DatabaseName=input_database)['TableList']

for table in tables:
    table_name = table['Name']
    
    datasource = glueContext.create_dynamic_frame.from_catalog(
        database=input_database,
        table_name=table_name,
        transformation_ctx="datasource"
    )

    df = datasource.toDF()


    flattened_df = df.select(
        col("numerocontrolepncp").alias("id_pncp"),
        col("objetocompra").alias("objeto_compra"),
        col("valortotalestimado").cast("double").alias("valor_estimado"),
        col("datapublicacaopncp").alias("data_publicacao_str"),
        
        
        col("orgaoentidade.razaosocial").alias("orgao_razao_social"),
        col("unidadeorgao.ufsigla").alias("uf"),
        col("unidadeorgao.municipionome").alias("municipio"),
        
        col("linksistemaorigem").alias("link_edital"),
        col("modalidadenome").alias("modalidade")
    )


    df_deduplicated = flattened_df.dropDuplicates(["id_pncp"])


    df_with_date = df_deduplicated.withColumn(
        "data_publicacao", 
        to_timestamp(col("data_publicacao_str"))
    )

    final_df = df_with_date \
        .withColumn('year', year(col('data_publicacao'))) \
        .withColumn('month', month(col('data_publicacao'))) \
        .withColumn('day', dayofmonth(col('data_publicacao'))) \
        .drop("data_publicacao_str")

    dynamic_frame = DynamicFrame.fromDF(final_df, glueContext, "dynamic_frame")


    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={
            "path": output_path,
            "partitionKeys": ["year", "month", "day"]
        },
        format="parquet"
    )

job.commit()