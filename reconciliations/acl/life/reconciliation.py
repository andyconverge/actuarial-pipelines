import pandas as pd
import csv
import pandas_gbq
import time
from datetime import datetime
import os
from google.oauth2 import service_account
from google.cloud import bigquery


CREDS = '../converge-database-0331482f2ee5.json'

client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)

expenses = ['']
result_df = pd.DataFrame()

def query_request(query, fieldname, set_month):
    job = client.query(query)
    print("Query making request: " , query)
    for j in job.result():
        result_row = {
            'set_month': set_month,
            'fieldname': fieldname,
            'withdrawal_amount': j[0],
        }
    print(result_row)
    return result_row


#Need to re-upload claims and include (ANN, IND, ORD), (Group or Individual), and net$payable as submitted payment. 
def claims(set_month):
    global result_df

    query = 'SELECT SUM(net_payable)*0.95 as surr \
            from `lifetemp.claims`\
            WHERE set_month = "'+set_month+'" AND adjudication_code = "SUR"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'surr', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(net_payable)*0.95 as death_i_ord\
            from `lifetemp.claims`\
            WHERE set_month = "'+set_month+'" and adjudication_code IN ("ANN", "WHL", "END") AND g_type ="I" AND p_type = "ORD" ' 
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'death_i_ord' , set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(net_payable)*0.95 as death_g_ord \
            from `lifetemp.claims`\
            WHERE set_month = "'+set_month+'" and adjudication_code IN ("ANN", "WHL", "END") AND g_type ="G" AND p_type = "ORD" ' 
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'death_g_ord',set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(net_payable)*0.95 as death_i_ind \
            from `lifetemp.claims`\
            WHERE set_month = "'+set_month+'" and adjudication_code IN ("ANN", "WHL" ,"END") AND g_type ="I" AND p_type = "IND" ' 
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query,'death_i_ind' , set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(net_payable)*0.95 as death_g_ind \
            from `lifetemp.claims`\
            WHERE set_month = "'+set_month+'" and adjudication_code IN ("ANN", "WHL", "END") AND g_type ="G" AND p_type = "IND" ' 
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'death_g_ind',  set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(net_payable)*0.95 as death_i_ann \
            from `lifetemp.claims`\
            WHERE set_month = "'+set_month+'" and adjudication_code IN ("ANN", "WHL", "END") AND g_type ="I" AND p_type = "ANN" ' 
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query,'death_i_ann' ,set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(net_payable)*0.95 as death_g_ann \
            from `lifetemp.claims`\
            WHERE set_month = "'+set_month+'" and adjudication_code IN ("ANN", "WHL","END") AND g_type ="G" AND p_type = "ANN" ' 
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'death_g_ann' ,set_month)])], ignore_index=True)
    
    print(query)



def premiums(set_month):
    global result_df

    query = 'SELECT SUM(net_premium)*0.95 as premium \
            from `lifetemp.premiums`\
            WHERE set_month = "'+str(set_month)+'"' 
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'premium', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(net_premium)*0.165*0.95 as percent_net_collected \
            from `lifetemp.premiums`\
            WHERE set_month = "'+str(set_month)+'"' 
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'percent_net_collected', set_month)])], ignore_index=True)
    
    #B section starts here
    #Administration expense ($12/12 x beginning of month per premium paying)
    
    #Early Pay -EYP
    query = 'SELECT SUM(wrvplcycnt)*0.95 from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="EYP" AND wrvgrpind ="I" and wrvclass ="IND"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_eyp_ind_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95 from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="EYP" AND wrvgrpind ="I" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_eyp_ind_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95 from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="EYP" AND wrvgrpind ="G" and wrvclass ="IND"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_eyp_ind_i', set_month)])], ignore_index=True)
    
    #Premium Payin - PRMPY
    
    query = 'SELECT SUM(wrvplcycnt)*0.95 from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="PRMPY" AND wrvgrpind ="I" and wrvclass ="ANN"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_prmpy_ann_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95 from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="PRMPY" AND wrvgrpind ="I" and wrvclass ="IND"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_prmpy_ind_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95 from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="PRMPY" AND wrvgrpind ="I" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_prmpy_ord_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95 from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="PRMPY" AND wrvgrpind ="G" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_prmpy_ord_g', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95 from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="PRMPY" AND wrvgrpind ="G" and wrvclass ="ANN"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_prmpy_ann_g', set_month)])], ignore_index=True)
    
    #Administration expense ($9/12 x beginning of month per paid up policy)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="FAN" AND wrvgrpind ="I" and wrvclass ="ANN"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_fan_ann_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="FPU" AND wrvgrpind ="I" and wrvclass ="IND"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_fpu_ind_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="FPU" AND wrvgrpind ="I" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_fpu_ord_i', set_month)])], ignore_index=True)
    
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="FPU" AND wrvgrpind ="G" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_FPU_ord_g', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="FAN" AND wrvgrpind ="G" and wrvclass ="ANN"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_FPU_ann_g', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="RPU" AND wrvgrpind ="I" and wrvclass ="IND"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_RPU_ind_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="RPU" AND wrvgrpind ="I" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_RPU_ord_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="RPU" AND wrvgrpind ="G" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_RPU_ord_g', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="WOD" AND wrvgrpind ="I" and wrvclass ="IND"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_WOD_ind_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="WOD" AND wrvgrpind ="I" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_WOD_ord_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="ETI" AND wrvgrpind ="I" and wrvclass ="IND"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_ETI_ann_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="ETI" AND wrvgrpind ="I" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_ETI_ord_i', set_month)])], ignore_index=True)
    
    query = 'SELECT SUM(wrvplcycnt)*0.95*(9/12) from `lifetemp.seriatim`\
            WHERE set_month ="'+str(set_month)+'" AND pmdstssub ="ETI" AND wrvgrpind ="G" and wrvclass ="ORD"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'admin_expense_ETI_ann_g', set_month)])], ignore_index=True)
    
    print(query)


def reserves(set_month):
    global result_df

    query = 'SELECT SUM(final_rsv)*0.95 as reserves \
            from `lifetemp.seriatim`\
            WHERE set_month = "'+str(set_month)+'"' 
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, 'reserves', set_month)])], ignore_index=True)
    print(query)


def run_reconciliation(set_month):
    global result_df
    result_df = pd.DataFrame()

    claims(str(set_month))
    premiums(set_month)
    reserves(set_month)
    print("----------------------------------------\done")

    result_df.to_excel('Reconciliation/LIFE_reconciliations'+str(set_month)+'.xlsx', index=False)