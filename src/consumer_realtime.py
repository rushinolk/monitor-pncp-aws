import json
import base64
import os
import boto3
import re
from botocore.exceptions import ClientError
from datetime import datetime

sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('editais_processados') 

# Variáveis de ambiente
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-1:423082503834:snsalerta'
MIN_VALUE = float(os.environ.get('MIN_VALUE', 100000))
TI_REGEX = r'\b(?:software|computador|microcomputador|notebook|desktop|servidor|storage|nuvem|cloud|SaaS|licença de software|TIC|tecnologia da informação|datacenter|backup|antivírus|firewall|switch|roteador|link dedicado|internet|outsourcing de impressão|inteligência artificial)\b'

def lambda_handler(event, context):
    if 'Records' not in event:
        print("Nenhum registro encontrado no evento.")
        return {
            'statusCode': 400,
            'body': json.dumps('No records found in the event')
        }

    for record in event['Records']:
        try:
            # Decodifica o dado do Kinesis (Base64 -> String -> JSON)
            payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            edital = json.loads(payload)

            # Extrai os campos de interesse (usando .get para evitar KeyError)
            id_pncp = edital.get('numeroControlePNCP')
            objeto = edital.get('objetoCompra', '')
            valor = float(edital.get('valorTotalEstimado', 0) or 0)
            orgao = edital.get('orgaoEntidade', {}).get('razaoSocial', 'Órgão não informado')
            uf = edital.get('unidadeOrgao', {}).get('ufSigla', 'N/A')
            link = edital.get('linkSistemaOrigem', 'Link não disponível')

            # Regra 1: Valor mínimo
            if valor < MIN_VALUE:
                continue

            # Regra 2: Regex de TI (case-insensitive)
            if not re.search(TI_REGEX, objeto, re.IGNORECASE):
                continue

            try:
                # Tenta buscar o ID no DynamoDB
                response = table.get_item(Key={'id_pncp': id_pncp})
                if 'Item' in response:
                    print(f"Edital {id_pncp} já foi processado. Ignorando para evitar spam.")
                    continue # Pula para o próximo registro do Kinesis
            except ClientError as e:
                print(f"Erro ao ler do DynamoDB: {e}")

            # Se passou por todas as regras, monta e envia a mensagem
            mensagem = (
                f"🚨 OPORTUNIDADE DE TI ENCONTRADA!\n\n"
                f"Órgão: {orgao} ({uf})\n"
                f"Valor Estimado: R$ {valor:,.2f}\n"
                f"Objeto: {objeto}\n\n"
                f"Acesse o edital: {link}"
            )

            sns_response = sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Message=mensagem,
                Subject=f"Oportunidade TI: {orgao} - R$ {valor:,.2f}"
            )
            print(f"Alerta enviado para o SNS. MessageId: {sns_response['MessageId']}")

            data_hora_atual = datetime.now().isoformat()


            table.put_item(Item={'id_pncp': id_pncp, 'data_processamento': data_hora_atual})
            print(f"Alerta enviado e ID {id_pncp} salvo no DynamoDB.")

        except Exception as e:
            print(f"Erro ao processar registro: {e}")
            # Continua para o próximo registro, não quebra a Lambda inteira
            continue

    return {
        'statusCode': 200,
        'body': json.dumps('Registros do Kinesis processados com sucesso.')
    }