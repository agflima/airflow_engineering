from airflow.decorators import dag, task
from datetime import timedelta,datetime
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from loguru import logger
import os

default_args = {
    'owner':'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

@dag(
    dag_id = "rfb_carga",
    default_args=default_args,
    description='DAG que realiza download e carga das informações públicas disponibilizadas pela RFB.',
    start_date=datetime.datetime(2024,8,19),
    schedule= "@daily",
    max_consecutive_failed_dag_runs=3,
    catchup=False
)
def rfb_dag():
    
    @task
    def download_arquivos():
        url = r"https://dadosabertos.rfb.gov.br/CNPJ/dados_abertos_cnpj/2024-08/"
        response = urlopen(url)
        html = response.read().decode('utf-8')
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            arquivo = link.get('href')
            st_arq = str(arquivo)
            if not (st_arq.startswith('?') or st_arq.startswith('/')):
                try:
                    start = datetime.now()
                    logger.info(f'Baixando arquivo: {arquivo}')
                    response = urlopen(url+arquivo)
                    with open(arquivo,'wb') as arq:
                        arq.write(response.read())
                    end = datetime.now()
                    logger.info(f'{arquivo} baixado em: {end-start} segundos')
                except Exception as e:
                    end = datetime.now()
                    logger.error(f'Erro ao baixar arquivo: {arquivo}. Tempo de execução: {end-start} segundos')
        return 'Arquivos baixados com sucesso!'

    # @task(task_id="move_arquivos")
    # def move_arquivos():

    download_arquivos()

rfb_dag()