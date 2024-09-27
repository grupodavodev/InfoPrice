#wesley 07.08.22 - integracao com InfoPrice

#Importacao das bibliotecas uteis
import requests
import json
from datetime import datetime,timedelta
import os
import cx_Oracle
import time
import csv
import paramiko #pip instal paramiko - envio de SFTP
import logging
from dotenv import load_dotenv 
load_dotenv() #Carregar variaveis 

###################################################################################### 1- VARIAVEIS GLOBAIS - INICIO
#
#

if os.name == 'nt': #windows
    dirLOGREQUEST = os.getenv("iDIRLOG_WIN") 
    dirCSV = os.getenv("iDIRCSV_WIN") 
else:
    os.environ['ORACLE_HOME'] = os.getenv("iDIRORACLE_LINUX") 
    dirLOGREQUEST = os.getenv("iDIRLOG_LINUX") 
    dirCSV =  os.getenv("iDIRCSV_LINUX") 

#LOG
iNAMEARQLOG = os.getenv("iNAMELOG") 
iEXTENSAO_LOG = os.getenv("iEXTENSAOLOG") 
logging.basicConfig(
    filename=f"{dirLOGREQUEST}{iNAMEARQLOG}_{datetime.now().strftime('%d%m%Y')}{iEXTENSAO_LOG}",  # Nome do arquivo de log
    format='%(asctime)s - [PID:%(process)d] -  %(levelname)s - %(funcName)s - %(message)s ',  # Formato da mensagem de log
    level=logging.DEBUG  # Nivel minimo de log que sera registrado
)

#Conexao Oracle PRD
try:
    myCONNORA = cx_Oracle.connect(f'{os.getenv("iUSER_ORA")}/{os.getenv("iPASS_ORA")}@{os.getenv("iHOST_ORA")}') 
    myCONNORA.autocommit = True
    curORA = myCONNORA.cursor() 
    curORA.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS= ',.' ")
except:
    print("erro ao conectar oracle")
    exit()

#SFTP
iHOST = os.getenv("iHOSTSFTP_ENV") 
portaSFTP = os.getenv("portaSFTP_ENV") 
userSFTP = os.getenv("userSFTP_ENV") 
passSFTP = os.getenv("passSFTP_ENV") 

#variaveis gerais
iDIA_EXTRACAO_VENDA = str((datetime.now() - timedelta(days=1)).strftime('%y%m%d'))
iLISTA_COD_ITENSVALIDOS = []
iNOME_CSV_INICIAL = os.getenv("iINICIAIS_NAME_CSV") 
iNOME_CSV_DATA = str(datetime.now().strftime('%Y-%m-%d'))


#
#
###################################################################################### 1- VARIAVEIS GLOBAIS - FIM


###################################################################################### 2- FUNCOES - INICIO
#
#

def enviaSFTP(iDIRCOMPLETO,iNOMEARQUIVO):
    logging.info("Funcao, envia o dado para o SFTP")
    conecta_sftp(iHOST,portaSFTP,userSFTP,passSFTP)
    iDE = iDIRCOMPLETO
    iPARA ="/home/" + str(userSFTP) + "/input/" + str(iNOMEARQUIVO)
    logging.debug(f"Enviando arquivo de ({iDE}) para ({iPARA})")
    sftp.put(iDE,iPARA)
    desconecta_sftp()
    logging.debug("Fim da funcao")

def conecta_sftp(iHOST,portaSFTP,userSFTP,passSFTP):
    logging.info("Funcao, faz a conexao com o SFTP")
    global sftp
    global transport
    try:
        transport = paramiko.Transport((str(iHOST),int(portaSFTP))) #host + porta
        transport.connect(None,str(userSFTP),str(passSFTP)) #usuario + senha
        sftp = paramiko.SFTPClient.from_transport(transport) #conecta sftp
    except IndexError as e: 
        logging.error(f"{e}") 
        desconecta_sftp()
    except paramiko.SSHException as  f:
        logging.error(f"{f}") 
        desconecta_sftp()

def desconecta_sftp(): 
    logging.info(f"Funcao, desconecta o SFTP")
    global sftp
    global transport
    try:
        if sftp: sftp.close()
        if transport: transport.close()
    except IndexError as e:  
        logging.error(f"{e}") 
        desconecta_sftp()
        pass
    except paramiko.SSHException as  f:
        logging.error(f"{f}") 
        desconecta_sftp()
        pass

def geraCSV(iTIPO,iLISTA):
    logging.info(f"Funcao, monta os dados de arquivo CSV. Parametros ({iTIPO}) - (iLISTA)")

    #produtos
    if iTIPO == 1:
        iNOMEARQ = "ProdutoCliente-"
        iLISTA_CABEC = ['date', 'product_id', 'product_id_type',
                        'product_id_package', 'product_id_unit', 'barcode',
                        'barcode_type', 'description', 'status',
                        'units_per_package', 'packing_size', 'packing_size_unit',
                        'packing_type', 'packing_type_unit', 'product_family_id',
                        'product_family_master', 'brand', 'supplier_id',
                        'abc_class', 'category_level_1', 'category_level_2',
                        'category_level_3', 'category_level_4', 'category_level_5',
                        'category_level_6', 'category_level_7', 'last_change']
    
    #fornecedores
    if iTIPO == 2:
        iNOMEARQ = "FornecedorCliente-"
        iLISTA_CABEC = ['date', 'supplier_id', 'supplier_type',
                        'supplier_name', 'cnpj', 'address',
                        'number', 'complement', 'neighbourhood',
                        'city', 'uf', 'cep',
                        'latitude', 'longitude', 'status',
                        'last_change']
    
    #precos
    if iTIPO == 3:
        iNOMEARQ = "PrecoCliente-"
        iLISTA_CABEC = ['date', 'store_id', 'channel',
                        'product_id', 'price_retail', 'price_wholesale',
                        'trigger_wholesale', 'min_competitivity', 'max_competitivity',
                        'max_margin', 'min_margin', 'objective_margin',
                        'sensibility_type', 'product_seasonal', 'last_change']
    
    #ofertas
    if iTIPO == 4:
        iNOMEARQ = "PromocaoClienteIntervalo-"
        iLISTA_CABEC = ['start_date', 'end_date', 'store_id',
                        'channel', 'product_id', 'promotion_price',
                        'promotion_name', 'promotion_type', 'client_cluster',
                        'last_change']
    
    #vendas
    if iTIPO == 5:
        iNOMEARQ = "VendaClienteAgregada-"
        iLISTA_CABEC = ['date', 'store_id', 'channel',
                        'product_id', 'sales_type', 'promotion_type',
                        'trigger_wholesale', 'client_cluster', 'unit_cost',
                        'taxes', 'margin_gross', 'margin_percentage',
                        'sales_amount', 'sales_gross', 'sales_net',
                        'discount', 'discount_type', 'total_cost']


    #monta CSV
    iNOME_ARQ_FTP = str(iNOME_CSV_INICIAL) + str(iNOMEARQ) + str(iNOME_CSV_DATA) + ".csv"
    iDIR_COMPLETO = str(dirCSV) + str(iNOME_ARQ_FTP) 
    logging.debug(f"iNOME_ARQ_FTP: {iNOME_ARQ_FTP}")   
    logging.debug(f"iDIR_COMPLETO: {iDIR_COMPLETO}")   
    with open(iDIR_COMPLETO, 'w', newline='', encoding='utf-8') as gravar:
        fieldnames = iLISTA_CABEC
        escrever = csv.DictWriter(gravar, fieldnames=fieldnames, delimiter='|')
        escrever.writeheader()
        for itens in iLISTA:
            iCONT_C_ITENS = 0
            iDICT = {}
            for cabec in iLISTA_CABEC:
                iDICT.update({cabec:itens[iCONT_C_ITENS]})
                iCONT_C_ITENS += 1
            escrever.writerow(iDICT) 
    logging.debug(f"Arquivo preparado, chamando funcao para envio do arquivo para o SFTP")
    enviaSFTP(iDIR_COMPLETO,iNOME_ARQ_FTP)
    logging.debug(f"Fim da funcao")

def montaFORNECEDOR():
    logging.info(f"Funcao, query que monta os dados de Fornecedor")
    iQUERY = ("""
                SELECT To_char(sysdate, 'YYYY-MM-DD')            AS data_integracao,
                t.tip_codigo
                || rms.Dac(t.tip_codigo)                  AS cod_for,
                CASE
                    WHEN t.tip_natureza IN ( 'FD' ) THEN 'DISTRIBUIDOR'
                    ELSE 'FORNECEDOR'
                END                                       tipo_for,
                t.tip_nome_fantasia                       AS nome_for,
                t.tip_cgc_cpf                             AS cnpj,
                t.tip_endereco                            AS endereco,
                0                                         AS num_endereco,
                ''                                        AS complemento,
                t.tip_bairro                              AS bairro,
                t.tip_cidade                              AS cidade,
                t.tip_estado                              AS uf,
                t.tip_cep                                 AS cep,
                0                                         AS latitude,
                0                                         AS longitude,
                'ATIVO'                                   AS status,
                To_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') AS data_mod
            FROM   rms.aa2ctipo t
            WHERE  t.tip_codigo IN (SELECT i.git_cod_for
                                    FROM   davo.infoprice_tmp tmp
                                        JOIN rms.aa3citem i
                                            ON ( i.git_cod_item = Trunc(tmp.cprod / 10) )) 
                                            
    """)
    logging.debug(f"{iQUERY}")
    try:
        iLISTA_FOR = []
        for iITEMS in curORA.execute(iQUERY).fetchall():
            iLISTA_FOR.append((iITEMS))
        geraCSV(2,iLISTA_FOR)            
    except cx_Oracle.DatabaseError as e_sql: 
        logging.error(f"{e_sql}")

def montaPRECOS():
    logging.info(f"Funcao, query que monta os dados de precos DAVO")
    iQUERY = ("""
         SELECT To_char(sysdate, 'YYYY-MM-DD')            AS data_integracao,
                    Trunc(est.get_cod_local / 10)             AS cod_loja,
                    'FISICA'                                  AS tp_local,
                    ean.ean_cod_pro_alt                       AS codigo,
                    rms.pc_rms_cal.F_calcstpr(1, ITM.git_cod_item, rms.Rms6to_rms7(
                                                                    rms.Dateto_rms(sysdate)),
                    Trunc(
                    est.get_cod_local / 10))                  AS preco_varejo,
                    CASE
                        WHEN ean.ean_apartir > 0 THEN
                        rms.pc_rms_cal.F_calcstpr(2, ITM.git_cod_item,
                        rms.Rms6to_rms7(
                        rms.Dateto_rms(sysdate)),
                        Trunc(est.get_cod_local / 10), 'A')
                        ELSE 0
                    END                                       preco_atacado,
                    ean.ean_apartir                           AS gatilho,
                    0                                         AS comp_minima,
                    0                                         AS comp_maxima,
                    0                                         AS margem_maxima,
                    0                                         AS margem_minima,
                    0                                         AS margem_obj,
                    ''                                        AS sensibilidade,
                    ''                                        AS sazonal,
                    To_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') AS data_mod
                FROM   rms.aa3ccean ean
                    JOIN rms.aa3citem itm
                        ON ( itm.git_codigo_ean13 = ean.ean_cod_ean )
                    JOIN rms.aa2cestq est
                        ON ( est.get_cod_produto = ean.ean_cod_pro_alt )
                    JOIN rms.aa2ctipo tip
                        ON ( tip.tip_codigo = Trunc(est.get_cod_local / 10) )
                    JOIN davo.infoprice_tmp tmp
                        ON ( ean.ean_cod_pro_alt = tmp.cprod )
                WHERE  Trunc(est.get_cod_local / 10) IN (SELECT t2.tip_codigo
                                                        FROM   rms.aa2ctipo t2
                                                        WHERE  t2.tip_regiao IN ( 2, 4, 8 ))
                ORDER  BY est.get_cod_local ASC 
                                  
    """)
    logging.debug(f"{iQUERY}")
    try:
        iLISTA_PRC = []
        for iITEMS in curORA.execute(iQUERY).fetchall():
            iLISTA_PRC.append((iITEMS))
        geraCSV(3,iLISTA_PRC)            
    except cx_Oracle.DatabaseError as e_sql: 
        logging.error(f"{e_sql}")

def montaOFERTAS():
    logging.info(f"Funcao, query que monta os dados de ofertas DAVO")
    iQUERY = ("""
         SELECT *
                FROM   (SELECT To_char(rms.Rmsto_date(rms.pc_rms_cal.F_calcstpr(6,
                                                    ITM.git_cod_item,
                                                                    rms.Rms6to_rms7(
                                                    rms.Dateto_rms(sysdate)), Trunc(
                                                            est.get_cod_local / 10)))
                            , 'YYYY-MM-DD')                           AS data_inicio,
                            To_char(rms.Rmsto_date(rms.pc_rms_cal.F_calcstpr(7,
                                                    ITM.git_cod_item,
                                                            rms.Rms6to_rms7(
                rms.Dateto_rms(sysdate)),
                Trunc(
                est.get_cod_local / 10))),
                'YYYY-MM-DD')                             AS data_fim,
                Trunc(est.get_cod_local / 10)             AS cod_loja,
                'FÍSICA'                                 AS canal_venda,
                ean.ean_cod_pro_alt                       AS codigo,
                rms.pc_rms_cal.F_calcstpr(3, ITM.git_cod_item, rms.Rms6to_rms7(
                rms.Dateto_rms(sysdate)),
                Trunc(
                est.get_cod_local / 10))                  AS preco_promocao,
                'PROMOCAO'                                AS nome_promocao,
                'DE/POR'                                  AS dinamica,
                ''                                        AS cluster_cli,
                To_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') AS data_mod
                FROM   rms.aa3ccean ean
                JOIN rms.aa3citem itm
                ON ( itm.git_codigo_ean13 = ean.ean_cod_ean )
                JOIN rms.aa2cestq est
                ON ( est.get_cod_produto = ean.ean_cod_pro_alt )
                JOIN rms.aa2ctipo tip
                ON ( tip.tip_codigo = Trunc(est.get_cod_local / 10) )
                JOIN davo.infoprice_tmp tmp
                ON ( ean.ean_cod_pro_alt = tmp.cprod )
                WHERE  Trunc(est.get_cod_local / 10) IN (SELECT t2.tip_codigo
                FROM   rms.aa2ctipo t2
                WHERE  t2.tip_regiao IN ( 2, 4, 8 ))
                ORDER  BY get_cod_local ASC) SE1
                WHERE  ( SE1.data_inicio IS NOT NULL
                        AND SE1.data_fim IS NOT NULL ) 
                                  
    """)
    logging.debug(f"{iQUERY}")
    try:
        iLISTA_PRC = []
        for iITEMS in curORA.execute(iQUERY).fetchall():
            iLISTA_PRC.append((iITEMS))
        geraCSV(4,iLISTA_PRC)            
    except cx_Oracle.DatabaseError as e_sql: 
        logging.error(f"{e_sql}")

def montaVENDAS():
    logging.info(f"Funcao, query que monta os dados de venda DAVO")
    iQUERY = ("""
        SELECT dta_venda,
                loja_sdig,
                canal_venda,
                codigo,
                tipo_venda,
                tip_promocao,
                gatilho,
                ident_cluster,
                custo,
                taxas,
                vlr_margem,
                ( vlr_margem * 100 ) / valor_venda,
                qtd_venda,
                valor_venda,
                valor_venda - taxas AS venda_liq,
                0                   AS valor_desconto,
                ''                  AS tip_desconto,
                custo               AS custo_total
            FROM   (SELECT To_char(rms.Rms7to_date(ien.eschc_data3), 'YYYY-MM-DD')
                                AS
                                dta_venda,
                        ien.eschljc_codigo3
                                AS loja_sdig,
                        'FÍSICA'
                                AS canal_venda,
                        itm.git_cod_item
                        || itm.git_digito
                                AS codigo,
                        'REGULAR'
                                AS tipo_venda,
                        ''
                                AS tip_promocao,
                        0
                                AS gatilho,
                        ''
                                AS ident_cluster,
                        ien.entsaic_cus_un * ien.entsaic_quanti_un
                                AS custo,
                        ( ien.entsaic_vlr_icm + ien.entsaic_valor_ipi ) *
                        ien.entsaic_quanti_un
                                AS taxas,
                        ( ien.entsaic_prc_un * ien.entsaic_quanti_un ) - (
                        ien.entsaic_cus_un * ien.entsaic_quanti_un )
                                AS vlr_margem,
                        ien.entsaic_quanti_un
                                AS qtd_venda,
                        ien.entsaic_prc_un * ien.entsaic_quanti_un
                                AS valor_venda
                    FROM   rms.ag1iensa ien
                        JOIN rms.aa3citem itm
                            ON ( itm.git_cod_item = ien.esitc_codigo )
                        JOIN davo.infoprice_tmp tmp
                            ON ( itm.git_cod_item = Trunc(tmp.cprod / 10) ) """)
    iQUERY += "   WHERE  ien.eschc_data3 = 1" + str(iDIA_EXTRACAO_VENDA)
    iQUERY += ("""                    AND ien.eschc_agenda IN ( 167, 195, 197, 198,
                                                    417, 428 )
                        AND ien.eschljc_codigo3 IN (SELECT t2.tip_codigo
                                                    FROM   rms.aa2ctipo t2
                                                    WHERE  t2.tip_regiao IN ( 2, 4, 8 ) )
                        AND itm.git_codigo_pai = 0) SE1
            ORDER  BY SE1.loja_sdig ASC 
                                  
    """)
    logging.debug(f"{iQUERY}")
    try:
        iLISTA_PRC = []
        for iITEMS in curORA.execute(iQUERY).fetchall():
            iLISTA_PRC.append((iITEMS))
        geraCSV(5,iLISTA_PRC)            
    except cx_Oracle.DatabaseError as e_sql: 
        logging.error(f"{e_sql}")


def limpaBASE_TMP(iLISTA_COD_ITENSVALIDOS):
    logging.info(f"Funcao, limpando base atual e atualizando lista de itens. Total de itens validos: {len(iLISTA_COD_ITENSVALIDOS)}")
    iQUERY = (" delete from davo.infoprice_tmp ")
    logging.debug(f"{iQUERY}")
    try:
        curORA.execute(iQUERY)
    except cx_Oracle.DatabaseError as e_sql: 
        logging.error(f"{e_sql}")
    
    for itens in iLISTA_COD_ITENSVALIDOS:
        iQUERY = (" insert into davo.infoprice_tmp (cprod) values (" + str(itens) + ") ")
        try:
            curORA.execute(iQUERY)
        except cx_Oracle.DatabaseError as e_sql: 
            logging.error(f"{e_sql}")


def captaITENSVALIDOS():
    logging.info(f"Funcao, capta codigo de itens validos para submeter a pesquisa de preco na InfoPrice")
    iQUERY = ("""
                SELECT To_char(sysdate, 'YYYY-MM-DD')            AS data_integracao,
                    ean.ean_cod_pro_alt                       AS codigo,
                    'SKU'                                     AS tipo_codigo,
                    ''                                        AS cod_dun,
                    ''                                        AS cod_interno_dun,
                    ean.ean_cod_ean                           AS codigo_barras,
                    CASE
                        WHEN ean.ean_tipo_ean IN ( 'C', 'D' ) THEN 'DUN'
                        WHEN ean.ean_tipo_ean NOT IN ( 'C', 'D', 'E' ) THEN 'INTERNO'
                        ELSE 'GTIN-13'
                    END                                       tipo_ean,
                    itm.git_descricao                         AS descricao_longa,
                    CASE
                        WHEN itm.git_dat_sai_lin > 0 THEN 'FORA DE LINHA'
                        ELSE 'ATIVO'
                    END                                       status,
                    itm.git_emb_for as unit_package,
                    itm.git_tpo_emb_for                       AS pack_size,
                    1 as pack_size_unit,
                    ean.ean_tpo_emb_venda                     AS tp_unidade,
                    ean.ean_emb_venda                         AS qtd_unidade,
                    itm.git_codigo_pai                        AS cod_pai,
                    CASE
                        WHEN Nvl((SELECT p.it_codigo
                                FROM   rms.aa1cheli p
                                WHERE  it_pai = ean.ean_cod_pro_alt
                                        AND rownum = 1), 0) > 0 THEN 'S'
                        ELSE 'N'
                    END                                       e_tipo_pai,
                    det.det_marca                             AS marca,
                    itm.git_cod_for
                    || rms.Dac(itm.git_cod_for)               AS cod_for,
                    itm.git_classe                            AS curva_abc,
                    (SELECT TAB.tab_conteudo
                        FROM   rms.aa2ctabe TAB
                        WHERE  TAB.tab_codigo = 16
                            AND To_number(Trim(TAB.tab_acesso)) = itm.git_depto
                            AND rownum = 1)                   AS merc_1,
                    (SELECT CM1.ncc_descricao descricao
                        FROM   rms.aa3cnvcc CM1
                        WHERE  CM1.ncc_departamento = itm.git_depto
                            AND CM1.ncc_secao = itm.git_secao
                            AND CM1.ncc_grupo = 0
                            AND CM1.ncc_subgrupo = 0
                            AND rownum = 1)                   AS merc_2,
                    (SELECT CM1.ncc_descricao descricao
                        FROM   rms.aa3cnvcc CM1
                        WHERE  CM1.ncc_departamento = itm.git_depto
                            AND CM1.ncc_secao = itm.git_secao
                            AND CM1.ncc_grupo = itm.git_grupo
                            AND CM1.ncc_subgrupo = 0
                            AND rownum = 1)                   AS merc_3,
                    (SELECT CM1.ncc_descricao descricao
                        FROM   rms.aa3cnvcc CM1
                        WHERE  CM1.ncc_departamento = itm.git_depto
                            AND CM1.ncc_secao = itm.git_secao
                            AND CM1.ncc_grupo = itm.git_grupo
                            AND CM1.ncc_subgrupo = itm.git_subgrupo
                            AND rownum = 1)                   AS merc_4,
                    (SELECT CM1.ncc_descricao descricao
                        FROM   rms.aa3cnvcc CM1
                        WHERE  CM1.ncc_departamento = itm.git_depto
                            AND CM1.ncc_secao = itm.git_secao
                            AND CM1.ncc_grupo = itm.git_grupo
                            AND CM1.ncc_subgrupo = itm.git_subgrupo
                            AND CM1.ncc_categoria = itm.git_categoria
                            AND rownum = 1)                   AS merc_5,
                    ''                                        AS merc_6,
                    ''                                        AS merc_7,
                    To_char(sysdate, 'YYYY-MM-DD HH24:MI:SS') AS data_mod
                FROM   rms.aa3ccean ean
                    JOIN rms.aa3citem itm
                        ON ( itm.git_cod_item = Trunc(ean.ean_cod_pro_alt / 10) )
                    LEFT JOIN rms.aa3citem itmpai
                            ON ( itmpai.git_cod_item = itm.git_codigo_pai )
                    JOIN rms.aa1ditem det
                        ON ( det.det_cod_item = Trunc(ean.ean_cod_pro_alt / 10) )
                    JOIN rms.aa2ctabe tab
                        ON ( tab.tab_codigo = 001
                            AND tab.tab_acesso = Lpad(itm.git_comprador, 3, '0')
                                                || '       ' )
                    JOIN rms.aa2ctipo tip
                        ON ( tip.tip_codigo = itm.git_cod_for )
                WHERE  ean.ean_pdv != 'N'
                    AND ean.ean_cod_ean = itm.git_codigo_ean13
                    AND itm.git_polit_pre = 'P'
    """)
    logging.debug(f"{iQUERY}")
    try:
        iLISTA_PRODUTOS = []
        for iITEMS in curORA.execute(iQUERY).fetchall():
            iLISTA_COD_ITENSVALIDOS.append(iITEMS[1])
            iLISTA_PRODUTOS.append((iITEMS))
            #break
        if len(iLISTA_COD_ITENSVALIDOS) > 0:
            geraCSV(1,iLISTA_PRODUTOS)
            montaFORNECEDOR()
            montaPRECOS()
            montaOFERTAS()
            montaVENDAS()
    except cx_Oracle.DatabaseError as e_sql: 
        logging.error(f"{e_sql}")
    


#
#
###################################################################################### 2- FUNCOES - FIM


###################################################################################### 3- QUERYES - INICIO
#
#


#
#
###################################################################################### 3- QUERYES - FIM



###################################################################################### 4- ACOES - INICIO
#
#

captaITENSVALIDOS()

#
#
###################################################################################### 4- ACOES - FIM