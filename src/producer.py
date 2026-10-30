import json
import requests
import boto3
from datetime import datetime, timedelta

# Configurações
URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
STREAM_NAME = "broker"
REGION = "us-east-1"
#hoje = datetime.now().strftime("%Y%m%d")
ontem = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")


params = {
    "dataInicial": ontem,
    "dataFinal": ontem,
    "codigoModalidadeContratacao": 6,  # Pregão Eletrônico
    "pagina": 1,
    "tamanhoPagina": 50
}

headers = {"accept": "application/json"}




kinesis_client = boto3.client('kinesis', region_name=REGION)

def lambda_handler(event, context):
    try:
        # Faz a chamada à API
        response = requests.get(URL, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        dados = response.json()
        editais = dados.get("data", [])
        
        total_enviado = 0
        
        # Envia cada edital para o Kinesis
        for edital in editais:
            
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(edital),
                PartitionKey="partition_key"
            )
            total_enviado += 1
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'{total_enviado} editais enviados ao Kinesis.')
        }
        
    except Exception as e:
        print(f"Erro: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Falha na execução: {str(e)}')
        }