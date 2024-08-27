from airflow.decorators import dag, task
from datetime import timedelta,datetime
from urllib.request import urlopen, urlretrieve
from bs4 import BeautifulSoup
from loguru import logger
import patoolib
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
    tags=["RFB","Publico"],
    start_date=datetime(2024,8,19),
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
    
    @task
    def descompacta_arquivos():
        for attr in os.listdir():
            if (attr.startswith('Empresa') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Empresas"
            elif (attr.startswith('Cnae') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Cnaes"
            elif (attr.startswith('Estab') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Estabelecimentos"
            elif (attr.startswith('Mot') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Motivos"
            elif (attr.startswith('Muni') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Municipios"
            elif (attr.startswith('Nat') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Naturezas"
            elif (attr.startswith('Pai') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Paises"
            elif (attr.startswith('Qua') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Qualificacoes"
            elif (attr.startswith('Si') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Simples"
            elif (attr.startswith('So') and attr.endswith('.zip')):
                output = "/mnt/c/data/RFB/Socios"
            else:
                continue

            try:
                patoolib.extract_archive(archive=attr,outdir=output)
                logger.info(f'Arquivo {attr}, extraído com sucesso!')
            except Exception as e:
                logger.error(f'Erro {e} ao extrair o arquivo {attr} para {output}.')
                return f'Erro: {e}, ao processar o arquivo {attr}!'
        return 'Arquivos processados com sucesso.'
    
    download_arquivos() >> descompacta_arquivos()

rfb_dag()