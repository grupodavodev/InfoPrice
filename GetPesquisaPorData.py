
import requests
import json
import cx_Oracle
from datetime import datetime,timedelta
import os
import math
import logging
from dotenv import load_dotenv 
load_dotenv() #Carregar variaveis 
import time

#busca pesquisas do dia formato = yyyy/mm/dd
iDATAINICIAL_BUSCA = (datetime.today() + timedelta(days=-4) ).strftime('%Y/%m/%d') 
iDATAFINAL_BUSCA =datetime.today().strftime('%Y/%m/%d')


iURLBASE = os.getenv("iURLBASE_INFO") 
iUSER_PRD = os.getenv("iUSERINFO") 
iPASS_PRD = os.getenv("iTKN_INFO") 
iBASICTKN_PRD = os.getenv("iBASIC_AUTH_INFO")

if os.name == 'nt': #windows
    dirLOGREQUEST = os.getenv("iDIRLOG_WIN") 
else:
    os.environ['ORACLE_HOME'] = os.getenv("iDIRORACLE_LINUX")  
    dirLOGREQUEST = os.getenv("iDIRLOG_LINUX") 


#LOG
iNAMEARQLOG = os.getenv("iNAMELOG_GETPESQUISA") 
iEXTENSAO_LOG = os.getenv("iEXTENSAOLOG") 
logging.basicConfig(
    filename=f"{dirLOGREQUEST}{iNAMEARQLOG}_{datetime.now().strftime('%d%m%Y')}{iEXTENSAO_LOG}",  # Nome do arquivo de log
    format='%(asctime)s - [PID:%(process)d] -  %(levelname)s - %(funcName)s - %(message)s ',  # Formato da mensagem de log
    level=logging.DEBUG  # Nivel minimo de log que sera registrado
)

#Conexao Oracle PRD
myCONNORA = cx_Oracle.connect(f'{os.getenv("iUSER_ORA")}/{os.getenv("iPASS_ORA")}@{os.getenv("iHOST_ORA")}') 
myCONNORA.autocommit = True
curORA = myCONNORA.cursor() #execucoes Oracle
curORA.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ',.' ")
curORA.execute("alter session set nls_date_format = 'DD/MM/YYYY'")    

def getTOKEN():
    logging.info(f"Funcao, capta token")
    try:
        url = iURLBASE + "/portal-acesso-web/oauth/token"
        method = "POST"
        params = {
            "grant_type": "password",
            "username": str(iUSER_PRD),
            "password": str(iPASS_PRD)
        }
        headers = {
            "Authorization": "Basic " + str(iBASICTKN_PRD)
        }

        response = requests.request(method, url, headers=headers, params=params)
        return json.loads(response.text)["access_token"]
    except Exception as e:
        logging.error(f"{e}")
        return ""

def excluiHISTORICO():
    logging.info(f"Exclui historico do BD, para evitar duplicidade de registros")
    try:
        iDINI =  str(iDATAINICIAL_BUSCA)[8:10] + "/" + str(iDATAINICIAL_BUSCA)[5:7] + "/" + str(iDATAINICIAL_BUSCA)[0:4]
        iDFIM =  str(iDATAFINAL_BUSCA)[8:10] + "/" + str(iDATAFINAL_BUSCA)[5:7] + "/" + str(iDATAFINAL_BUSCA)[0:4]
        iQUERY = (f"""
                     DELETE FROM INFOPRICE_EXTRACAO INF 
             WHERE INF.ESTAB_DATA BETWEEN ('{iDINI}') AND ('{iDFIM}') 
        """)
        logging.debug(f"{iQUERY}")
        try:
            curORA.execute(iQUERY)                  
        except cx_Oracle.DatabaseError as e_sql: 
            print("Erro iQUERY_CLI: " + str(e_sql))
            pass
    except Exception as e:
        logging.error(f"{e}")
        pass

def buscaQTDPAGINAS():
    logging.info(f"Funcao, busca a quantidade de paginas de pesquisa vigente")
    try:

        #url = iURLBASE + "/integracao/v2/relatorio?dataInicio=" + str(iDATAINICIAL_BUSCA) + "&dataFim=" + str(iDATAFINAL_BUSCA) + "&page=0"
        url = iURLBASE + "/integracao/v3/relatorio?dataInicio=" + str(iDATAINICIAL_BUSCA) + "&dataFim=" + str(iDATAFINAL_BUSCA) + "&page=0"

        payload={}
        headers = {
            "Authorization": "Bearer " + str(getTOKEN())
            ,"Content-Type": "application/json"
        }
        logging.debug(f"url: {url}")
        response = requests.request("GET", url, headers=headers, data=payload)
        logging.debug(f"response.status_code: {response.status_code}")
        iJSON = json.loads(response.text)
        print(f"Total de paginas: {iJSON['totalPages']}")
        logging.debug(f"Total de paginas: {iJSON['totalPages']}")
        return iJSON['totalPages']
    except:
        print(f"Err. Total de paginas: 01")
        return 1

def trataJSON(iJSON):
    logging.info(f"Funcao, trata o JSON recebido no requesta da InfoPrice")
    try:
        #print(iJSON)
        for itens in iJSON['content']:
            iDATA = itens['data']
            iDATA_TRATADA = str(iDATA)[8:10] + "/" + str(iDATA)[5:7] + "/" + str(iDATA)[0:4]
            iLOJA = itens['loja']
            iCOD = itens['produto']
            iPRECO_NORMAL = itens['preco_varejo']
            iPRECO_ATACADO = itens['preco_atacado']
            if iPRECO_ATACADO == None:
                iPRECO_ATACADO = 0 
            iPROMOCAO = "False"
            if str(itens['promocao']) == "True" or str(itens['rebaixa_preco']) == "True"  :
                iPROMOCAO = "True"
            
            #ATUALIZACAO V3
            iGATILHOATACADO = 0
            if "gatilho_atacado" in itens: 
                if itens['gatilho_atacado'] != None: iGATILHOATACADO = itens['gatilho_atacado']
            iREBAIXAPRECO = ""
            if "rebaixa_preco" in itens: iREBAIXAPRECO = itens['rebaixa_preco']
            iCLUBEDESCONTO = ""
            if "clube_desconto" in itens: iCLUBEDESCONTO = itens['clube_desconto']
            iPRECODE = "0"
            if "preco_de" in itens: 
                if itens['preco_de'] != None: 
                    try:
                        iPRECODE = itens['preco_de']
                    except:
                        pass
            iPRECOPOR = "0"
            if "preco_por" in itens: 
                if itens['preco_por'] != None: 
                    try:
                        iPRECOPOR = itens['preco_por']
                    except:
                        pass
            iDTVALIDADE = ""
            if "data_validade" in itens: 
                if itens['data_validade'] != None: iDTVALIDADE = itens['data_validade']
            iAUDITORIA = ""
            if "auditoria" in itens: 
                if itens['auditoria'] != None: iAUDITORIA = itens['auditoria']
            iSUGESTAO = ""
            if "sugestao" in itens: 
                if itens['sugestao'] != None: iSUGESTAO = itens['sugestao']
            iESCOPO = ""
            if "escopo" in itens: 
                if itens['escopo'] != None: iESCOPO = itens['escopo']


            if iPRECO_NORMAL != None:
                iQUERY = (f""" 
                        INSERT INTO davo.infoprice_extracao
                                    (estab_data,             estab_descricao,             prod_cod,
                                    prod_preco,             prod_preco_atacado,             prod_promocao,
                                    data_extracao,             prod_itm7,
                                        gatilho_atacado ,rebaixa_preco ,clube_desconto,
                                            preco_de,preco_por  , data_validade  ,
                                            auditoria  ,sugestao, escopo    
                                     )
                        VALUES      ('{iDATA_TRATADA}',             '{iLOJA}',             {iCOD},
                                    {iPRECO_NORMAL},             {iPRECO_ATACADO},             '{iPROMOCAO}',
                                    sysdate,             {str(iCOD)[0:len(iCOD)-1]},
                                    '{iGATILHOATACADO}',             '{iREBAIXAPRECO}',             '{iCLUBEDESCONTO}',
                                    {iPRECODE},             {iPRECOPOR},             '{iDTVALIDADE}',
                                    '{iAUDITORIA}',             '{iSUGESTAO}', '{iESCOPO}'
                                    ) 
                    """)
                logging.debug(f"{iQUERY}")
                try:
                    curORA.execute(iQUERY)                  
                except cx_Oracle.DatabaseError as e_sql: 
                    print("Erro iQUERY_CLI: " + str(e_sql))
                    pass
                
    except Exception as e:
        logging.error(f"{e}")
        print(f"{e}")
        pass


def extraiINF(iPAGE):
    logging.info(f"Funcao , faz o requesta da pagina. Parametros ({iPAGE})")
    #url = iURLBASE + "/integracao/v2/relatorio?dataInicio=" + str(iDATAINICIAL_BUSCA) + "&dataFim=" + str(iDATAFINAL_BUSCA) + "&page=" + str(iPAGE)
    url = iURLBASE + "/integracao/v3/relatorio?dataInicio=" + str(iDATAINICIAL_BUSCA) + "&dataFim=" + str(iDATAFINAL_BUSCA) + "&page=" + str(iPAGE)

    payload={}
    headers = {
            "Authorization": "Bearer " + str(getTOKEN())
            ,"Content-Type": "application/json"
        }
    logging.debug(f"{url}")
    response = requests.request("GET", url, headers=headers, data=payload)
    iJSON = json.loads(response.text)

    if response.status_code != 200: #print(response.status_code)
        print(iJSON)
    else:
        if iPAGE ==0:
            excluiHISTORICO()
    trataJSON(iJSON)

iCONTADOR = 0
iQTDPAGINAS_EXTRACAO = buscaQTDPAGINAS()
while True:
    logging.debug(f"iniciando pagina: {iCONTADOR}")
    print(f"iniciando pagina: {iCONTADOR}")
    if iCONTADOR < iQTDPAGINAS_EXTRACAO:
        extraiINF(iCONTADOR)
        iCONTADOR += 1
    else:
        break
    if iCONTADOR>= 999: 
        logging.warning(f"Alerta, indicio de erro iCONTADOR: {iCONTADOR}")
        break

try:
    myCONNORA.close()
    curORA.close()
except:
    pass
