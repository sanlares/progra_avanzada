from datetime import datetime
from airflow.models.dag import DAG
import pandas as pd
from airflow.operators.python import PythonOperator
import boto3
from dotenv import load_dotenv
from io import StringIO
import os

def filtrar_advertisers_activos():
    
    bucket_name='trabajoprogramacion'
    ACCESS_KEY = ('AKIATCKANBM2M4RMYV6Y')
    SECRET_KEY=('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')
    s3 = boto3.client( 's3',region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    #DESCARGO LOS DATOS DE S3
    def download_from_s3(file_name):
        response=s3.get_object(Bucket=bucket_name,Key=file_name)
        return pd.read_csv(response['Body'])
    
    #funcion para subir datos a s3
    def upload_to_s3(df,file_name):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)
        s3.put_object(Bucket=bucket_name, Key=file_name, Body= csv_buffer.getvalue())
        
    #nombres de los archivos en S3
    ads_views = 'ads_views'
    advertiser_ids = 'advertiser_ids'
    product_views = 'product_views'
    
    #descargar dataframes
    df_ads_views = download_from_s3(ads_views)
    df_advertisers = download_from_s3(advertiser_ids)
    df_product_views = download_from_s3(product_views)
    

    # Filtrar publicistas activos
    df_active_advertisers = df_advertisers[df_advertisers['status'] == 'activo']
    df_active_products_views = df_product_views[df_product_views['advertiser_id'].isin(df_active_advertisers['advertiser_id'])]
    df_active_ads_views = df_ads_views[df_ads_views['advertiser_id'].isin(df_active_advertisers['advertiser_id'])]
    
    #genera el nombre de los archivos
    today_date = datetime.now().strftime('%Y-%m-%d')
    file_active_ads_views = f'active_ads_views_{today_date}.csv'
    file_active_advertisers = f'active_advertisers_{today_date}.csv'
    file_active_product_views = f'active_product_views_{today_date}.csv'
    
    #subo los df filtrados en S3
    upload_to_s3(df_active_ads_views,file_active_ads_views)
    upload_to_s3(df_active_advertisers,file_active_advertisers)
    upload_to_s3(df_active_products_views,file_active_product_views)
    
    return 'Data upload successfully'



#top CTR por cada advertiser se muestran los productos con mejor CTR de los ads en las diferentes paginas web
#el CTR indica la frecuencia con la que las personas que ven un anucio terminan haciendo clic en el. Es una forma simple de evaluar que tan bien estan funcionando tu anuncios y cuand acticos son para la audiencia.
#CTR= (numero de clicks/numero de impresiones)*100

def top_ctr():
    #datos AWS
    bucket_name='trabajoprogramacion'
    ACCESS_KEY = ('AKIATCKANBM2M4RMYV6Y')
    SECRET_KEY=('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')
    s3 = boto3.client( 's3',region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    #genero nombre del archivo
    today_date=datetime.now().strftime('%Y-%m-%d')
    file_name=f'active_ads_views_{today_date}.csv'
    
    #descargo los dataframes desde S3
    def download_from_s3(bucket_name,file_name):
        response = s3.get_object(Bucket=bucket_name,Key=file_name)
        return pd.read_csv(response['Body'])
    
    df_active_ads_views = download_from_s3(bucket_name,file_name)
    
    #procesar top ctr
        #filtro por fecha hoy
    
    df_today = df_active_ads_views[df_active_ads_views['date'] == today_date]
    
    #filtro clicks e impresiones para la fecha de hoy
    clicks_today= df_today[df_today['type']=='click']
    impressions_today= df_today[df_today['type']=='impression']

         #filtro clicks e impresiones    
    clicks = clicks_today[clicks_today['type']=='click'].groupby(['advertiser_id','product_id','date']).size().rename('clicks')
    impressiones = impressions_today[impressions_today['type']=='impression'].groupby(['advertiser_id','product_id','date']).size().rename('impressions')
    
    #uno los clicks y las impresiones
    ctr_data = pd.concat([clicks,impressiones], axis=1)
    ctr_data.fillna(0,inplace=True) #me aseguro que no haya nan en los datos
    
    #calculo CTR
    ctr_data['CTR'] = (ctr_data['clicks']/ctr_data['impressions'])*100
    
    ctr_data.reset_index(inplace=True)
    
    #ordeno y selecciono el top 20 de los productos por CTR para cada advertiser
    top_20_ctr = ctr_data.groupby('advertiser_id',group_keys=False).apply(lambda x: x.nlargest(20, 'CTR'))
    top_20_ctr_filtrado = top_20_ctr[(top_20_ctr['CTR'] > 0) & (top_20_ctr['CTR'] !=float('inf'))]
  
    
    #   Subo el archivo a s3
    
    def upload_to_s3(df,bucket_name,file_name):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key =file_name, Body=csv_buffer.getvalue())
        
    result_file_name = f'top_20_ctr_filtrado_{today_date}.csv'
    upload_to_s3(top_20_ctr_filtrado,bucket_name,result_file_name)
    
    return'Top 20 ctr data upload successfully'


#def top_ctr():
    #datos AWS
    bucket_name='trabajoprogramacion'
    ACCESS_KEY = ('AKIATCKANBM2M4RMYV6Y')
    SECRET_KEY=('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')
    s3 = boto3.client( 's3',region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    #genero nombre del archivo
    today_date=datetime.now().strftime('%Y-%m-%d')
    file_name=f'active_ads_views_{today_date}.csv'
    
    #descargo los dataframes desde S3
    def download_from_s3(bucket_name,file_name):
        response = s3.get_object(Bucket=bucket_name,Key=file_name)
        return pd.read_csv(response['Body'])
    
    df_active_ads_views = download_from_s3(bucket_name,file_name)
    
    #procesar top ctr
        #filtro por fecha hoy
    
    df_today = df_active_ads_views[df_active_ads_views['date'] == today_date]
    
    #filtro clicks e impresiones para la fecha de hoy
    clicks_today= df_today[df_today['type']=='click']
    impressions_today= df_today[df_today['type']=='impression']

         #filtro clicks e impresiones    
    clicks = clicks_today[clicks_today['type']=='click'].groupby(['advertiser_id','product_id','date']).size().rename('clicks')
    impressiones = impressions_today[impressions_today['type']=='impression'].groupby(['advertiser_id','product_id','date']).size().rename('impressions')
    
    #uno los clicks y las impresiones
    ctr_data = pd.concat([clicks,impressiones], axis=1)
    ctr_data.fillna(0,inplace=True) #me aseguro que no haya nan en los datos
    
    #calculo CTR
    ctr_data['CTR'] = (ctr_data['clicks']/ctr_data['impressions'])*100
    
    ctr_data.reset_index(inplace=True)
    
    #ordeno y selecciono el top 20 de los productos por CTR para cada advertiser
    top_20_ctr = ctr_data.groupby('advertiser_id',group_keys=False).apply(lambda x: x.nlargest(20, 'CTR'))
    top_20_ctr_filtrado = top_20_ctr[(top_20_ctr['CTR'] > 0) & (top_20_ctr['CTR'] !=float('inf'))]
    
      
    return top_20_ctr_filtrado

#prueba = top_ctr()   
#print(prueba)
 
#tpo product
def top_product():

    #datos AWS
    bucket_name='trabajoprogramacion'
    ACCESS_KEY = ('AKIATCKANBM2M4RMYV6Y')
    SECRET_KEY=('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')
    s3 = boto3.client( 's3',region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    #genero nombre del archivo
    today_date=datetime.now().strftime('%Y-%m-%d')
    file_name=f'active_product_views_{today_date}.csv'
    
    #descargo los dataframes desde S3
    def download_from_s3(bucket_name,file_name):
        response = s3.get_object(Bucket=bucket_name,Key=file_name)
        return pd.read_csv(response['Body'])
    
    df_active_product_views = download_from_s3(bucket_name,file_name)
 
 #proceso top product
            #filtro por fecha hoy
    
    df_today = df_active_product_views[df_active_product_views['date'] == today_date]   
    
    #cuento las vistas por cada combinacion de advertiser_id y product_id y por fecha
    product_views = df_today.groupby(['advertiser_id','product_id', 'date']).size().rename('visitas')
    
    #ordeno y selecciono los top 20 productos por vistas para cada advertiser
    
    top_productos = product_views.reset_index().sort_values(["advertiser_id","date","visitas"], ascending=[True,True, False])
    top_productos = top_productos.groupby(["advertiser_id","date"]).head(20)    
    
     #   Subo el archivo a s3
    
    def upload_to_s3(df,bucket_name,file_name):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key =file_name, Body=csv_buffer.getvalue())
        
    result_file_name = f'top_productos_{today_date}.csv'
    upload_to_s3(top_productos,bucket_name,result_file_name)
    
    return'Top 20 product data upload successfully'
 
#dbwirting
import psycopg2

def create_tables_if_not_exist():
    # Me conecto a la base de datos
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='conesacolegiales',
        host='programacion.cpusky0oqvsv.us-east-2.rds.amazonaws.com',
        port=5432
    )
    cur = conn.cursor()
    
    # Verifico si la tabla top_ctr_table ya existe
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'top_ctr_table')")
    top_ctr_table_exists = cur.fetchone()[0]
    
    # Si la tabla top_ctr_table no existe, la creo
    if not top_ctr_table_exists: #date DATEagregarla 
        cur.execute('''
            CREATE TABLE top_ctr_table (
                advertiser_id VARCHAR(255),
                product_id VARCHAR(255),
                date DATE,
                clicks INTEGER,
                impressions INTEGER,
                CTR FLOAT
            )
        ''')
    
    # Verifico si la tabla top_products_table ya existe
    cur.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'top_products_table')")
    top_products_table_exists = cur.fetchone()[0]
    
    # Si la tabla top_products_table no existe, la creo
    if not top_products_table_exists:
        cur.execute('''
            CREATE TABLE top_products_table (
                advertiser_id VARCHAR(255),
                product_id VARCHAR(255),
                date DATE,
                visitas INTEGER
            )
        ''')
    
    # Confirmo y cierro la conexión
    conn.commit()
    cur.close()
    conn.close()



def write_to_database():
    
    #me conecto a la base de datos
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='conesacolegiales',
        host='programacion.cpusky0oqvsv.us-east-2.rds.amazonaws.com',
        port=5432
    )
    cur = conn.cursor()
    
    #datos AWS
    bucket_name='trabajoprogramacion'
    ACCESS_KEY = ('AKIATCKANBM2M4RMYV6Y')
    SECRET_KEY=('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')
    s3 = boto3.client( 's3',region_name='us-east-1', aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY)
    
    #fecha actual 
    today_date = datetime.now().strftime('%Y-%m-%d')
    
    #nombre de los archivos en S3
    
    top_20_ctr_filtrado_filename = f'top_20_ctr_filtrado_{today_date}.csv'
    top_productos_20_filename = f'top_productos_{today_date}.csv'
    

    #DESCARGO LOS DATOS DE S3
    def download_from_s3(file_name):
        response=s3.get_object(Bucket=bucket_name,Key=file_name)
        return pd.read_csv(response['Body'])
    
        
    df_top_ctr = download_from_s3(top_20_ctr_filtrado_filename)
    df_top_product = download_from_s3(top_productos_20_filename)
 
    
    #escribo resultado de la base de datos
    for ctr_result in df_top_ctr.itertuples(): #AGREGAR DATE
        cur.execute('INSERT INTO top_ctr_table (advertiser_id , product_id,date,clicks,impressions, CTR) VALUES(%s,%s,%s,%s,%s,%s)', (ctr_result.advertiser_id, ctr_result.product_id,ctr_result.date,ctr_result.clicks,ctr_result.impressions,ctr_result.CTR))
        
    for product_result in df_top_product.itertuples():
        cur.execute('INSERT INTO top_products_table (advertiser_id , product_id, date, visitas) VALUES (%s, %s,%s,%s)', (product_result.advertiser_id, product_result.product_id,product_result.date,product_result.visitas))
        
    #confirmo y cierro conexcion
    conn.commit()
    cur.close()
    conn.close()
    

 

with DAG(
        dag_id='advertising_data_pipeline',
         schedule_interval='@daily',
         start_date = datetime(2024,4,30),
         catchup=False) as dag:
    
    
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
    
    create_tables_if_not_exist_task = PythonOperator(
        task_id='create_tables_if_not_exist',
        python_callable = create_tables_if_not_exist,
        provide_context=True        
    )
    
    write_to_database_task =PythonOperator(
        task_id='write_database',
        python_callable =write_to_database,
        provide_context=True
    )


    task_filtrar_datos>> [task_top_ctr,task_top_product] >>create_tables_if_not_exist_task>>write_to_database_task
