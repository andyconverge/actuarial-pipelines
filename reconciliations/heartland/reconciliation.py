import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta


CREDS = '../converge-database-0331482f2ee5.json'
client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)

def query_function(set_month, plangroup, tablename, fieldname, withdrawal_type):
    
    if tablename =='premium':
        query = 'SELECT SUM('+fieldname+')*0.95 as '+tablename+'_'+plangroup+' FROM `heartland.'+tablename+'`\
                WHERE set_month ="'+set_month+'" AND plangroup ="'+plangroup+'"'
    elif tablename =='withdrawals':
        query = 'SELECT SUM('+fieldname+')*0.95 as '+tablename+'_'+plangroup+' FROM `heartland.'+tablename+'`\
                WHERE set_month ="'+set_month+'" AND plangroup ="'+plangroup+'" AND withdrawal_type ="'+withdrawal_type+'"'
    print(query)
    job = client.query(query)
    for j in job.result():
        result_row = {
            'set_month' : set_month,
            'plan_group' : plangroup,
            'type': fieldname if tablename =='premium' else withdrawal_type,
            'total': j[0],
        }   
        print(result_row)
    return result_row



def expenses(set_month, plangroup, expense_type):
    if expense_type == 'issue':
        query = f'''
        SELECT (count(*)*180) as issue_expense_{plangroup}
        FROM `heartland.seriatim`
        WHERE set_month = "{set_month}" AND policy_number NOT IN 
        (SELECT policy_number from `heartland.seriatim` WHERE set_month ='{get_previous_month(set_month)}')
        AND plangroup = "{plangroup}"
        '''
    elif expense_type == 'admin':
        query = f'''
        SELECT (count(*)*(120/12)*POWER(1.02, ({set_month[:4]}-2023))) as admin_expense_{plangroup}
        FROM `heartland.seriatim`
        WHERE set_month = "{set_month}" AND plangroup = "{plangroup}"
        '''
    job = client.query(query)
    for j in job.result():
        result_row = {
            'set_month' : set_month,
            'plan_group': plangroup,
            'type' : expense_type,
            'total': j[0],
        }   
        print(result_row)
    return result_row



def get_previous_month(set_month):
    # Parse the set_month string into a datetime object
    date = datetime.strptime(set_month, "%Y%m")
    
    # Subtract one month
    previous_month_date = date - relativedelta(months=1)
    
    # Format the result back into the "YYYYMM" string format
    previous_month = previous_month_date.strftime("%Y%m")
    
    return previous_month



def other(set_month, plangroup, typeres):
    if typeres == 'reserves':
        query = f'''
            SELECT SUM(stat_reserve*0.95) from `heartland.seriatim`
            WHERE set_month = "{set_month}" AND plangroup = "{plangroup}"
            '''
    elif typeres == 'interest':
         query = f'''
            SELECT SUM(interest_credited+bonus_credited)*0.95 from `heartland.seriatim`
            WHERE set_month = "{set_month}" AND plangroup = "{plangroup}"
            '''
    elif typeres == 'policy_deduction':
        query = f'''
            SELECT SUM(expense_charges)*0.95 from `heartland.seriatim`
            WHERE set_month = "{set_month}" AND plangroup = "{plangroup}"
            '''
    job = client.query(query)
    for j in job.result():
        result_row = {
            'set_month' : set_month,
            'plan_group': plangroup,
            'type' : typeres,
            'total': j[0],
        } 
    print(result_row)
    return result_row

def run_reconciliation(set_month):
    plangroup = ['MYGE03', 'MYGE05', 'MYGE07', 'MYGE10']

    premium_tables = ['total_premium', 'renewal_premium']

    withdrawals = ['Full Surrender Withdrawals', 'Partial Withdrawal with SC', 'RMD Withdrawals', 'Free Interest Credit Withdrawals', 'Freelook Withdrawals', 'Cancellation Withdrawals', 'Death Benefit', 'Enhanced Benefit Withdrawals', 'Free Partial Withdrawals']

    
    print("Running reconciliation for: ", set_month)
    
    result_df = pd.DataFrame()
    
    
    #Premium
    for i in plangroup:
        for j in premium_tables:
            result_df = pd.concat([result_df, pd.DataFrame([(query_function(set_month, i,"premium", j, ""))])], ignore_index=True)
    print('------------------')         
    #Withdrawals
    
    for i in plangroup:
        for j in withdrawals:
            result_df = pd.concat([result_df, pd.DataFrame([(query_function(set_month, i,"withdrawals", "withdrawal_amount", j))])], ignore_index=True)
    print('------------------')        
    #Commissions
    
    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame([(query_function(set_month, i,"premium", "initial_commission", ""))])], ignore_index=True)
    print('------------------')    
    # Issue Expense
    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame([expenses(set_month, i, "issue")])], ignore_index=True)
    print('------------------')    
    # Admin Expense
    
    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame([expenses(set_month, i, "admin")])], ignore_index=True)
    print('------------------')    
    
    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame([(other(set_month, i, 'reserves'))])], ignore_index=True)
        
    print('------------------')    
    
    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame([other(set_month, i, 'interest')])], ignore_index=True)
        
    print('------------------')     
    
    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame([other(set_month, i, 'policy_deduction')])], ignore_index=True)
        
        
    
    result_df.to_excel('Heartland_reconciliation_result_'+str(set_month)+'.xlsx', index=False)