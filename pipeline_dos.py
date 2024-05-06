from datetime import datetime
from airflow.models.dag import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
import boto3
from dotenv import load_dotenv
from io import StringIO
import os

#df_advertisers = pd.read_csv("advertiser_ids") #contiene advertisers activos en inactivos
#df_ads_views = pd.read_csv("ads_views") #contiene los ads de los advertisers activos cuyas publicidades fueron impression o clikeadas.
#df_product_views = pd.read_csv("product_views") #dice los productos visitados por cada avertisers

###############esto para usar boto3
    
def import_raw(bucket = 'trabajoprogramacion'):
    load_dotenv()  # Carga las variables de entorno del archivo .env

    ACCESS_KEY = ('AKIATCKANBM2M4RMYV6Y')
    SECRET_KEY=('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')

    s3 = boto3.client( 's3',region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    bucket_name = bucket 

    ads_views = 'ads_views'
    file_object_ads_views = s3.get_object(Bucket=bucket_name, Key=ads_views) 
    df_ads_views = pd.read_csv(file_object_ads_views['Body'])


    advertiser_ids = 'advertiser_ids'
    file_object_advertiser_ids = s3.get_object(Bucket=bucket_name, Key=advertiser_ids) 
    df_advertisers = pd.read_csv(file_object_advertiser_ids['Body'])


    product_views = 'product_views'
    file_object_product_views = s3.get_object(Bucket=bucket_name, Key=product_views) 
    df_product_views = pd.read_csv(file_object_product_views['Body'])

    return df_ads_views, df_advertisers, df_product_views


def load_data(ti):
    df_ads_views, df_advertisers, df_product_views =import_raw()
    
    ti.xcom_push(key='df_advertisers', value=df_advertisers.to_json(orient='split'))
    ti.xcom_push(key='df_product_views', value=df_product_views.to_json(orient='split'))
    ti.xcom_push(key='df_ads_views', value=df_ads_views.to_json(orient='split'))
    #return df_advertisers,df_product_views, df_ads_views


def filtrar_advertisers_activos(ti):
    
    df_advertisers_json = ti.xcom_pull(task_ids='load_data', key='df_advertisers')
    df_product_views_json = ti.xcom_pull(task_ids='load_data', key='df_product_views')
    df_ads_views_json = ti.xcom_pull(task_ids='load_data', key='df_ads_views')
    
    df_advertisers = pd.read_json(df_advertisers_json, orient='split')
    df_product_views = pd.read_json(df_product_views_json, orient='split')
    df_ads_views = pd.read_json(df_ads_views_json, orient='split')

    # Filtrar publicistas activos
    df_active_advertisers = df_advertisers[df_advertisers['status'] == 'activo']
    df_active_products_views = df_product_views[df_product_views['advertiser_id'].isin(df_active_advertisers['advertiser_id'])]
    df_active_ads_views = df_ads_views[df_ads_views['advertiser_id'].isin(df_active_advertisers['advertiser_id'])]
    
    ti.xcom_push(key='df_active_products_views', value=df_active_products_views.to_json(orient='split'))
    ti.xcom_push(key='df_active_ads_views', value=df_active_ads_views.to_json(orient='split'))
    


#top CTR por cada advertiser se muestran los productos con mejor CTR de los ads en las diferentes paginas web
#el CTR indica la frecuencia con la que las personas que ven un anucio terminan haciendo clic en el. Es una forma simple de evaluar que tan bien estan funcionando tu anuncios y cuand acticos son para la audiencia.
#CTR= (numero de clicks/numero de impresiones)*100

def top_ctr(ti):
    df_active_ads_views_json = ti.xcom_pull(task_ids='filtrar_datos', key='df_active_ads_views')
    df_active_ads_views = pd.read_json(df_active_ads_views_json, orient='split')
    
    #filtro clicks e impresiones
    
    clicks = df_active_ads_views[df_active_ads_views['type']== 'click'].groupby(['advertiser_id','product_id']).size().rename('clicks')
    impressiones = df_active_ads_views[df_active_ads_views['type']=='impression'].groupby(['advertiser_id','product_id']).size().rename('impressions')
    
    #uno los clicks y las impresiones
    ctr_data = pd.concat([clicks,impressiones], axis=1)
    ctr_data.fillna(0,inplace=True) #me aseguro que no haya nan en los datos
    
    #calculo CTR
    ctr_data['CTR'] = (ctr_data['clicks']/ctr_data['impressions'])*100
    
    #ordeno y selecciono el top 20 de los productos por CTR para cada advertiser
    top_20_ctr = ctr_data.groupby('advertiser_id').apply(lambda x: x.nlargest(20, 'CTR')).reset_index(level=0, drop=True)
    
    return top_20_ctr



def top_product(ti):
    df_active_products_views_json = ti.xcom_pull(task_ids='filtrar_datos', key='df_active_ads_views')
    df_active_products_views = pd.read_json(df_active_products_views_json, orient='split')
 
    
    #cuento las vistas por cada combinacion de advertiser_id y product_id y por fecha
    product_views = df_active_products_views.groupby(['advertiser_id','product_id', 'date']).size().rename('visitas')
    
    #ordeno y selecciono los top 20 productos por vistas para cada advertiser
    
    top_productos = product_views.reset_index().sort_values(["advertiser_id","date","visitas"], ascending=[True,True, False])
    top_productos = top_productos.groupby(["advertiser_id","date"]).head(20)    
    
    return top_productos


with DAG(
        dag_id='advertising_data_pipeline',
         schedule_interval='@daily',
         start_date = datetime(2024,4,30),
         catchup=False) as dag:
    
    import_raw_task=PythonOperator(
        task_id='import_raw',
        python_callable=import_raw,
        op_kwargs={'bucket':'trabajoprogramacion'}
    )
    
    task_load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        provide_context=True
    )
    
    task_filtrar_datos = PythonOperator(
        task_id='filtrar_datos',
        python_callable=filtrar_advertisers_activos,
        provide_context=True
    )
    
    task_top_ctr = PythonOperator(
        task_id='top_ctr',
        python_callable=top_ctr,
        provide_context=True
    )

    task_top_product = PythonOperator(
        task_id='top_product',
        python_callable=top_product,
        provide_context=True
    )


    import_raw_task>>task_load_data>>task_filtrar_datos>> [task_top_ctr,task_top_product]

        #t4 = PostgresOperator(
     #   task_id='db_writing',
      #  sql="sql/write_to_db.sql",  # Asumiendo que tienes un script SQL preparado para insertar los datos
       # postgres_conn_id="postgres_default"
    #)