import requests
import pandas as pd
from datetime import datetime, timedelta
import re
import time

def explorar_ti_nacional_100k():
    base_url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"
    
    data_fim = datetime.now()
    # Se quiser buscar mais dados, pode manter 60 dias, mas 30 já costuma dar um bom volume
    data_inicio = data_fim - timedelta(days=1) 
    
    params = {
        "dataInicial": data_inicio.strftime("%Y%m%d"),
        "dataFinal": data_fim.strftime("%Y%m%d"),
        "codigoModalidadeContratacao": 6, # Pregão Eletrônico
        "pagina": 1,
        "tamanhoPagina": 50
    }
    
    print("⏳ Baixando dados do PNCP (Nacional)...")
    todos_editais = []
    
    # Baixar até 40 páginas (2000 registros)
    for pagina in range(1, 41):
        params["pagina"] = pagina
        
        max_tentativas = 3
        for tentativa in range(max_tentativas):
            try:
                response = requests.get(base_url, params=params, timeout=30)
                
                # Se tomar bloqueio (429), espera 5 segundos e tenta de novo
                if response.status_code == 429:
                    print(f"  [Aviso] Rate limit na página {pagina}. Aguardando 5s...")
                    time.sleep(5)
                    continue
                
                response.raise_for_status()
                dados = response.json()
                editais = dados.get("data", [])
                
                if not editais:
                    break # Fim dos dados
                    
                todos_editais.extend(editais)
                if pagina % 10 == 0:
                    print(f"  {len(todos_editais)} registros baixados com sucesso...")
                
                break # Sucesso na página, sai do loop de tentativas
                
            except Exception as e:
                if tentativa == max_tentativas - 1:
                    print(f"  [Erro] Falha definitiva na página {pagina}: {e}")
                time.sleep(3)
        
        # Pausa entre as páginas para não ser bloqueado
        time.sleep(2) 

    if not todos_editais:
        print("Nenhum dado para analisar.")
        return

    # --- Transformando em DataFrame ---
    dados_limpos = []
    for e in todos_editais:
        dados_limpos.append({
            "id_pncp": e.get("numeroControlePNCP"),
            "objeto": e.get("objetoCompra", ""),
            "valor_estimado": float(e.get("valorTotalEstimado", 0) or 0),
            "data_publicacao": pd.to_datetime(e.get("dataPublicacaoPncp")),
            "orgao": e.get("orgaoEntidade", {}).get("razaoSocial", "Não informado"),
            "uf": e.get("unidadeOrgao", {}).get("ufSigla", "N/A"),
            "municipio": e.get("unidadeOrgao", {}).get("municipioNome", "N/A")
        })
        
    df = pd.DataFrame(dados_limpos)
    print(f"\n📊 Volume total de editais baixados: {len(df)}")

    # --- FUNIL DE FILTRAGEM ---
    
    # Regex com (?:...) para remover o aviso do Pandas
    padrao_ti = r'\b(?:software|computador|microcomputador|notebook|desktop|servidor|storage|nuvem|cloud|SaaS|licença de software|TIC|tecnologia da informação|datacenter|backup|antivírus|firewall|switch|roteador|link dedicado|internet|outsourcing de impressão|inteligência artificial)\b'
    
    df_ti = df[df["objeto"].str.contains(padrao_ti, case=False, na=False, regex=True)]
    print(f"🔍 Editais contendo termos de TI: {len(df_ti)}")

    # Filtro de Valor (> R$ 100.000)
    df_ti_100k = df_ti[df_ti["valor_estimado"] >= 100000]
    print(f"🎯 Editais de TI com valor >= R$ 100.000: {len(df_ti_100k)}")
    
    if df_ti_100k.empty:
        print("Nenhum edital passou no filtro final. Tente aumentar o período de busca.")
        return

    # --- EXPORTAÇÃO ---
    nome_arquivo = "editais_ti_nacional_100k.csv"
    df_ti_100k.to_csv(nome_arquivo, index=False, encoding='utf-8-sig')
    print(f"\n✅ Dataset final (Gold) salvo em '{nome_arquivo}'.")
    print("   Este arquivo está pronto para ser usado no seu dashboard.")

if __name__ == "__main__":
    explorar_ti_nacional_100k()