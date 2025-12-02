import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta


CREDS = '../converge-database-0331482f2ee5.json'
client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)
set_month = "202510"

plangroup = ['MYG03', 'MYG04', 'MYG05',
             'MYG06', 'MYG07', 'MYG08', 'MYG09', 'MYG10']

premium_tables = ['total_premium', 'renewal_premium']

withdrawals = ['Full Surrender Withdrawals', 'Partial Withdrawal with SC', 'RMD Withdrawals', 'Free Interest Credit Withdrawals',
               'Freelook Withdrawals', 'Cancellation Withdrawals', 'Death Benefit', 'Enhanced Benefit Withdrawals', 'Free Partial Withdrawals']


def query_function(set_month, plangroup, tablename, fieldname, withdrawal_type):

    if tablename == 'premium':
        query = 'SELECT SUM('+fieldname+')*0.5 as '+tablename+'_'+plangroup+' FROM `kskj.'+tablename+'`\
                WHERE set_month ="'+set_month+'" AND plangroup ="'+plangroup+'"'
    elif tablename == 'withdrawals':
        query = 'SELECT SUM('+fieldname+')*0.5 as '+tablename+'_'+plangroup+' FROM `kskj.'+tablename+'`\
                WHERE set_month ="'+set_month+'" AND plangroup ="'+plangroup+'" AND withdrawal_type ="'+withdrawal_type+'"'
    print(query)
    job = client.query(query)
    for j in job.result():
        result_row = {
            'set_month': set_month,
            'plan_group': plangroup,
            'type': fieldname if tablename == 'premium' else withdrawal_type,
            'total': j[0],
        }
        print(result_row)
    return result_row


def expenses(set_month, plangroup, expense_type):
    if expense_type == 'issue':
        query = f'''
        SELECT ((COUNT(*) * 37.5) * 0.5) AS issue_expense_{plangroup}
        FROM `kskj.premium`
        WHERE set_month = '{set_month}' 
          AND plangroup = '{plangroup}'
        '''
    elif expense_type == 'admin':
        query = f'''
        SELECT ((count(*)*(20/12)*POWER(1.03, ({set_month[:4]}-2024))*0.5)) as admin_expense_{plangroup} 
        FROM `kskj.premium`
        WHERE set_month = "{set_month}" AND plangroup = "{plangroup}"
        '''
    print(query)
    job = client.query(query)
    for j in job.result():
        result_row = {
            'set_month': set_month,
            'plan_group': plangroup,
            'type': expense_type,
            'total': j[0],
        }
        print(result_row)
    return result_row


def get_previous_month(set_month):

    date = datetime.strptime(set_month, "%Y%m")

    previous_month_date = date - relativedelta(months=1)

    previous_month = previous_month_date.strftime("%Y%m")

    return previous_month


def other(set_month, plangroup, typeres):
    if typeres == 'reserves':
        query = f'''
            SELECT SUM(stat_reserve*0.5) from `kskj.seriatim`
            WHERE set_month = "{set_month}" AND plangroup = "{plangroup}"
            '''
    elif typeres == 'interest':
        query = f'''
            SELECT SUM(interest_credited+bonus_credited)*0.5 from `kskj.seriatim`
            WHERE set_month = "{set_month}" AND plangroup = "{plangroup}"
            '''
    elif typeres == 'policy_deduction':
        query = f'''
            SELECT SUM(expense_charges)*0.5 from `kskj.seriatim`
            WHERE set_month = "{set_month}" AND plangroup = "{plangroup}"
            '''
    job = client.query(query)
    for j in job.result():
        result_row = {
            'set_month': set_month,
            'plan_group': plangroup,
            'type': typeres,
            'total': j[0],
        }
    print(result_row)
    return result_row


def run_reconciliation(set_month):
    print("Running reconciliation for KSKJ", set_month)
    result_df = pd.DataFrame()

    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame(
            [expenses(set_month, i, "issue")])], ignore_index=True)

    # Premium
    for i in plangroup:
        for j in premium_tables:
            result_df = pd.concat([result_df, pd.DataFrame(
                [(query_function(set_month, i, "premium", j, ""))])], ignore_index=True)

    # Withdrawals

    for i in plangroup:
        for j in withdrawals:
            result_df = pd.concat([result_df, pd.DataFrame([(query_function(
                set_month, i, "withdrawals", "withdrawal_amount", j))])], ignore_index=True)

    # Commissions

    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame([(query_function(
            set_month, i, "premium", "initial_commission", ""))])], ignore_index=True)

    # Issue Expense

    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame(
            [expenses(set_month, i, "issue")])], ignore_index=True)

    # Admin Expense

    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame(
            [expenses(set_month, i, "admin")])], ignore_index=True)

    # Reserves

    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame(
            [(other(set_month, i, 'reserves'))])], ignore_index=True)

    # Interest

    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame(
            [other(set_month, i, 'interest')])], ignore_index=True)

    # Policy deduction

    for i in plangroup:
        result_df = pd.concat([result_df, pd.DataFrame(
            [other(set_month, i, 'policy_deduction')])], ignore_index=True)
    print("exporting the result")
    result_df.to_excel('Query Result/KSKJ_reconciliation_result_' +
                       str(set_month)+'.xlsx', index=False);
    
    
