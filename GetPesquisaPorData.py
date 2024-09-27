
import requests
import json
import cx_Oracle
from datetime import datetime,timedelta
import os
import math
import logging

#busca pesquisas do dia formato = yyyy/mm/dd
iDATAINICIAL_BUSCA = (datetime.today() + timedelta(days=-4) ).strftime('%Y/%m/%d') 
iDATAFINAL_BUSCA =datetime.today().strftime('%Y/%m/%d')
#iDATAINICIAL_BUSCA = "2022/10/17"
#iDATAFINAL_BUSCA = "2022/10/21"


iURLBASE = "https://api.infopriceti.com.br"
iUSER_PRD = "integracao.davo@infoprice.co"
iPASS_PRD = "af04c71bf442f756a5aa5ab508b5ef574a9c258f66ba71f99e970b9d111365925c06b24cf88d27b08b47239f7e190468c3855354cb9795403eeeb604f3b77041"

if os.name == 'nt': #windows
    dirLOGREQUEST = "//nas/dbxprd/PRD/LOG/" 
else:
    os.environ['ORACLE_HOME'] = "/usr/lib/oracle/19.6/client64"   
    dirLOGREQUEST = "/dbx/PRD/LOG/" 


#LOG
iNAMEARQLOG = "InfoPrice_GetPesquisa_Data" 
iEXTENSAO_LOG = ".log"
logging.basicConfig(
    filename=f"{dirLOGREQUEST}{iNAMEARQLOG}_{datetime.now().strftime('%d%m%Y')}{iEXTENSAO_LOG}",  # Nome do arquivo de log
    format='%(asctime)s - [PID:%(process)d] -  %(levelname)s - %(funcName)s - %(message)s ',  # Formato da mensagem de log
    level=logging.DEBUG  # Nivel minimo de log que sera registrado
)

#Conexao Oracle PRD
myCONNORA = cx_Oracle.connect('davo/d4v0@davoprd') #conexao com o Oracle
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
            "Authorization": "Basic bXktdHJ1c3RlZC1jbGllbnQ6c2VjcmV0"
        }

        response = requests.request(method, url, headers=headers, params=params)
        #print(json.loads(response.text)["access_token"])
        return json.loads(response.text)["access_token"]
    except Exception as e:
        logging.error(f"{e}")
        return ""

def excluiHISTORICO():
    try:
        iDINI =  str(iDATAINICIAL_BUSCA)[8:10] + "/" + str(iDATAINICIAL_BUSCA)[5:7] + "/" + str(iDATAINICIAL_BUSCA)[0:4]
        iDFIM =  str(iDATAFINAL_BUSCA)[8:10] + "/" + str(iDATAFINAL_BUSCA)[5:7] + "/" + str(iDATAFINAL_BUSCA)[0:4]
        iQUERY = (" "+ 
        "             DELETE FROM INFOPRICE_EXTRACAO INF "+ 
        "     WHERE INF.ESTAB_DATA BETWEEN ('" + str(iDINI) + "') AND ('" + str(iDFIM) + "') "+ 
        " ")
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
    try:

        url = iURLBASE + "/integracao/v2/relatorio?dataInicio=" + str(iDATAINICIAL_BUSCA) + "&dataFim=" + str(iDATAFINAL_BUSCA) + "&page=0"

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
            
            if iPRECO_NORMAL != None:
                iQUERY = (" " +
                "             insert into INFOPRICE_EXTRACAO  " +
                "     (estab_data, estab_descricao, prod_cod, " +
                "     prod_preco, prod_preco_atacado, prod_promocao, data_extracao, prod_itm7) values " +
                "     ('" + str(iDATA_TRATADA) + "', '" + str(iLOJA) + "', " + str(iCOD) + ", " +
                "     " + str(iPRECO_NORMAL) + ", " + str(iPRECO_ATACADO) + " , '" + str(iPROMOCAO) + "', sysdate, " + str(iCOD)[0:len(iCOD)-1] + ") " +
                " ")
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
    url = iURLBASE + "/integracao/v2/relatorio?dataInicio=" + str(iDATAINICIAL_BUSCA) + "&dataFim=" + str(iDATAFINAL_BUSCA) + "&page=" + str(iPAGE)

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
    print("iniciando pagina: " + str(iCONTADOR))
    if iCONTADOR < iQTDPAGINAS_EXTRACAO:
        extraiINF(iCONTADOR)
        iCONTADOR += 1
    else:
        break
    if iCONTADOR>= 999: 
        logging.warning(f"Alerta, indicio de erro iCONTADOR: {iCONTADOR}")
        break #indicio de erro

try:
    myCONNORA.close()
    curORA.close()
except:
    pass
