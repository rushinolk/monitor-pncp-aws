# Monitor de Licitações Públicas (PNCP) - Foco em TI

## 📌 Contexto de Negócio
Este projeto resolve um problema real de consultorias e empresas de Tecnologia da Informação que vendem para o governo. O Portal Nacional de Contratações Públicas (PNCP) publica milhares de editais diariamente, mas a esmagadora maioria é de obras, saúde ou papelaria. 

O processo manual de garimpar as raras e valiosas oportunidades de TI é lento, ineficiente e gera perda de prazos. A consequência não é apenas tempo perdido, é deixar de fechar contratos milionários porque o analista não conseguiu ler o edital a tempo.

**A Solução:** Um pipeline de dados Serverless na AWS que ingere dados do PNCP, filtra oportunidades reais de TI (acima de R$ 100.000), dispara alertas em tempo quase real via e-mail e constrói um histórico analítico do mercado.

## 🏗️ Arquitetura da Solução
O projeto foi construído com uma arquitetura orientada a eventos (Streaming) e processamento em lote (Batch), utilizando o padrão *fan-out* no broker.

*(Insira aqui a imagem do seu diagrama no Excalidraw: `./architecture/diagram.png`)*

*   **Ingestão:** O Amazon EventBridge agenda uma AWS Lambda (Producer) para consultar a API do PNCP a cada 15 minutos, respeitando o horário comercial.
*   **Broker:** Os dados brutos vão para o Amazon Kinesis Data Streams.
*   **Processamento (Fan-out):** O Kinesis distribui o mesmo evento para dois consumidores simultâneos:
    *   **Consumer Realtime:** Aplica um filtro cirúrgico (Regex de termos de TI + valor > R$ 100k). Se aprovado, verifica a Idempotência no DynamoDB e dispara o alerta via Amazon SNS.
    *   **Consumer Batch:** Persiste o JSON bruto no Amazon S3 (Camada Raw), garantindo a rastreabilidade total do dado.
*   **Camada Gold:** O AWS Glue (PySpark) lê a camada Raw, aplica o filtro de TI, deduplica os registros e grava em formato Parquet no S3. As consultas analíticas são feitas via Amazon Athena.

## 🛠️ Decisões Técnicas e Diferenciais
*   **Idempotência com DynamoDB:** Como a Lambda Producer roda a cada 5 minutos, o mesmo edital entraria no pipeline várias vezes. O DynamoDB atua como um cache no Consumer, garantindo que o mesmo alerta não seja enviado duas vezes (evitando a fadiga de alerta).
*   **Separação de Responsabilidades (Raw vs Gold):** A camada Raw armazena o espelho de tudo que é publicado. A filtragem de negócio e a deduplicação são aplicadas posteriormente via PySpark no Glue, garantindo que a camada Gold contenha apenas dados analíticos relevantes.
*   **Filtro Cirúrgico (Regex):** Uso de expressões regulares para evitar falsos positivos (ex: não confundir "Sistema de Informação" com "Sistema de Água").
*   **FinOps:** Para economizar recursos, o Streaming roda apenas em horário comercial, alinhado ao comportamento de publicação dos órgãos públicos.
*   **Resiliência:** A Lambda Producer possui lógica de *retry* com backoff exponencial para lidar com os bloqueios (HTTP 429) da API do governo.

## 📊 Resultados e Impacto (A Prova de Conceito)

**1. Alertas em Tempo Real (Camada Realtime):**
O sistema identificou e disparou e-mails imediatos para oportunidades reais de compras diretas. Em um dos testes em produção, o pipeline capturou um edital de **R$ 4.648.405,81** do Comando do Exército (RJ) para aquisição de material de TIC.

*(Insira aqui o print do e-mail do Exército: `./imagens/alerta_email.png`)*

**2. Inteligência de Mercado (Camada Gold):**
Após o processamento no AWS Glue (com deduplicação e filtro de termos de TI), a consulta analítica via Amazon Athena revelou o volume de oportunidades por estado. Na amostra processada, o estado do Pará (PA) concentrou **R$ 19,8 Milhões** em 4 oportunidades válidas de TI (já excluindo editais de obras e saúde).

*(Insira aqui o print da query no Athena: `./imagens/athena_resultado.png`)*

## 🚀 Tecnologias Utilizadas
*   **Cloud:** AWS (Lambda, Kinesis, S3, DynamoDB, SNS, Glue, Athena, EventBridge)
*   **Linguagens:** Python (Boto3, Requests, PySpark)
*   **Formatos de Dados:** JSON, Parquet, CSV
*   **Ferramentas:** Git / GitHub, Excalidraw 

## ⚙️ Como rodar este projeto
1. Faça o upload do código da Lambda Producer e configure as variáveis de ambiente.
2. Crie o Kinesis Data Stream e conecte os triggers nas Lambdas Consumer.
3. Crie a tabela no DynamoDB com a chave primária `id_pncp` (String).
4. Execute o Crawler no S3 (camada Raw) e rode o Job no Glue para gerar a camada Gold.
5. Execute o Crawler na camada gold e rode as consultas pelo Athena.