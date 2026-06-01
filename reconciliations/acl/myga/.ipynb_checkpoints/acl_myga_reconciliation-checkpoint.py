import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta

CREDS = '../../converge-database-0331482f2ee5.json'

client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)

result_df = pd.DataFrame()

def get_previous_month(set_month):
    # Parse the set_month string into a datetime object
    date = datetime.strptime(set_month, "%Y%m")
    
    # Subtract one month
    previous_month_date = date - relativedelta(months=1)
    
    # Format the result back into the "YYYYMM" string format
    previous_month = previous_month_date.strftime("%Y%m")
    
    return previous_month

def query_request(query, fieldname, set_month):
    job = client.query(query)
    for j in job.result():
        result_row = {
            'set_month': set_month,
            'fieldname': fieldname,
            'withdrawal_amount': j[0],
        }
    return result_row


#Withdrawals
def query_function(set_month, withdrawal_type):
    global result_df  
    if withdrawal_type == 'premium':
        field = 'premium_amount'
    else:
        field = 'withdrawal_amount'
    query = 'SELECT SUM(f.'+field+'*(CASE WHEN f.reins_code ="P" THEN 0.65 ELSE 0.4 END)) as sum_'+withdrawal_type+' \
            from `aclico.'+withdrawal_type+'` as f\
            JOIN `aclico.policy` p ON p.policy_number = f.policy_number\
            WHERE f.set_month = "'+set_month+'"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, withdrawal_type, set_month)])], ignore_index=True)
    print(query)

def commission(set_month):
    global result_df
    query = 'select SUM(paid*(\
          CASE WHEN reins_flag ="P" THEN 0.65 ELSE 0.4\
          END )) from `aclico.commissions` where set_month ="'+str(set_month)+'"'
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, "commission", set_month)])], ignore_index=True)
    print(query)
    
    
def expenses(set_month, prev_month):
  
    query =''
    global result_df
    query = 'WITH new_policies_by_flag AS (\
    SELECT sv.reins_flag ,count(*) as num from `aclico.seriatim_values` sv\
    where sv.policy_number NOT IN (select policy_number from `aclico.seriatim_values` where set_month ="'+str(prev_month)+'" ) AND sv.set_month ="'+str(set_month)+'"\
    GROUP by sv.reins_flag)\
    SELECT SUM(60*num*(CASE WHEN reins_flag ="P" THEN 0.65 ELSE 0.4 END)*\
        (CASE WHEN reins_flag IN ("AF", "P", "V") THEN 1.2 ELSE 1 END)) FROM new_policies_by_flag'
    print(query, '-------------------------------')
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, "issue_expense", set_month)])], ignore_index=True)
    
    #Marketing expense
    query = 'select ABS(sum(premium_amount*quota_share*0.0019)) from `aclico.premium` WHERE set_month ="'+str(set_month)+'"'
    print(query, '-------------------------------')
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, "marketing_expense", set_month)])], ignore_index=True)
    
    query = 'WITH surrender_expense AS (\
    SELECT reins_code, count(distinct policy_number) as num from `aclico.full_surrenders`\
    where set_month ="'+str(set_month)+'"\
    group by reins_code)\
    SELECT SUM(num*20*(\
              CASE WHEN reins_code ="P" THEN 0.65 ELSE 0.4 END )*\
              (CASE WHEN reins_code IN ("3C", "3D") THEN 1.04 ELSE 1.13 END)) FROM surrender_expense'
    print(query, '-------------------------------')
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, "surrender_expense", set_month)])], ignore_index=True)
    
    
    query = 'WITH death_claims AS (\
    SELECT reins_code, count(distinct policy_number) as num from `aclico.death_claims`\
    where set_month ="'+str(set_month)+'" group by reins_code)\
    SELECT SUM(num*50*(CASE WHEN reins_code ="P" THEN 0.65 ELSE 0.4 END\
            )*(CASE WHEN reins_code IN ("3C", "3D") THEN 1.04 ELSE 1.13 END)) FROM death_claims'
    print(query, '-------------------------------')
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, "death_claim_expense", set_month)])], ignore_index=True)
    
    query = 'select ABS(sum(premium_amount*quota_share*0.03))/2 from `aclico.premium` WHERE set_month ="'+str(set_month)+'"'
    print(query, '-------------------------------')
    result_df = pd.concat([result_df, pd.DataFrame([query_request(query, "additional/ceding_allowance", set_month)])], ignore_index=True)
    
    
def run_reconciliation(set_month):
    
    withdrawal_type = ['full_surrenders', 'partial_surrenders', 'aiw', 'rmd', 'cancellations', 'surrender_fees', 'penalty_free', 'death_claims', 'premium', 'premium_taxes']

    
    prev_month = get_previous_month(set_month)
    expenses(set_month, prev_month)
    for i in withdrawal_type:
        query_function(str(set_month), i)
    commission(set_month)
    
    print("END -------------------")
    
    result_df.to_excel('Query Results/reconciliations/MYGA_reconciliations_'+str(set_month)+'.xlsx', index=False)