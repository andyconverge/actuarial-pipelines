import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta


CREDS = '../converge-database-0331482f2ee5.json'
client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)
result_df = pd.DataFrame()
def withdrawals(set_month, withdrawal_type, product):
    global result_df 
    sum_field = "gross_premium" if withdrawal_type == 'premium' else "av_withdrawn"
    query = f'''
    select sum({sum_field}*quota_share) FROM `{product}.{withdrawal_type}` WHERE set_month = "{set_month}"
    '''
    print(query)
    job = client.query(query)
    for j in job.result():
        result_row = {
            'product' : product,
            'fieldname': withdrawal_type,
            'total': j[0],
        }   
        result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
        print(result_row)
    

def other(set_month, withdrawal_type, product):
    global result_df 
    query = f"""
    SELECT SUM(net_amount) AS total
    FROM `{product}.{withdrawal_type}`
    WHERE set_month = '{set_month}'
    """
    print(query)
    job = client.query(query)
    for j in job.result():
        result_row = {
            'product' : product,
            'fieldname': withdrawal_type,
            'total': j[0],
        }   
        result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
        print(result_row)
#Fix surrender charge on farmers FIA with 247 diff and product not labeled correctly.
def charges(set_month, account_number):
    global result_df 
    if account_number in ("1-C00-4650-101", "1-C00-4650-102"):
        field = "diff"
    else: field = "credit_amount"
    
    if account_number in ("1-C00-4650-102", "1-C00-5100-102"):
        product = "farmers_fia"
    else:
        product = "farmers"
    query = f"""
    SELECT SUM({field} * quota_share) AS total
    FROM `farmers.combined_gl`
    WHERE set_month = '{set_month}'
      AND account_number = '{account_number}'
      AND description IN (
          'Annuity Regular Distribution',
          'Annuity RMD',
          'Annuity Partial Surrender',
          'Annuity Full Surrender'
      )
    """
    print(query)
    withdrawal_type = "surrender_charge" if account_number in ("1-C00-5100-101", "1-C00-5100-102") else "mva"
    job = client.query(query)
    for j in job.result():
        result_row = {
            'product' : product,
            'fieldname': withdrawal_type,
            'total': j[0],
        }   
        result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
        print(result_row)
def admin_expense(product, set_month):
    global result_df
    if product =='farmers_fia':
        table_name = 'rsv'
        qs = 'converge_qs'
    else:
        table_name ='seriatim'
        qs ='quota_share'
    rsv_query = f'''select SUM(gross_reserve*{qs}) as total from `{product}.{table_name}`
                    WHERE set_month ='{set_month}'; '''
    job = client.query(rsv_query)
    result = job.result()

    # Fetch the first row safely
    row = next(result, None)

    # Handle case where no rows are returned
    total_value = row.total if row and row.total is not None else 0

    result_row = {
        "product" : product,
        "fieldname": "reserve",
        "total": total_value,
    }

    result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)


def calc_expense(set_month):
    global result_df
    query = f"""
    select (100*count(mpolicy)*0.20 +
    (select 100*count(mpolicy)*0.10 from `farmers.seriatim_new`
    WHERE set_month ='{set_month}' and new_policy_check ='NEW' and term=3) )
    as total
    from `farmers.seriatim_new`
    WHERE set_month ='{set_month}' and new_policy_check ='NEW' and term IN (5,7,10)
    """
    print(query)
    job = client.query(query)
    row = next(job.result(), None)
    total_value = row.total if row and row.total is not None else 0
    result_row = {"product": "farmers", "fieldname": "expense", "total": total_value}
    result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
    print(result_row)


def calc_cede(set_month, product):
    global result_df
    if product == 'farmers':
        query = f'''SELECT -SUM(diff*quota_share*CASE WHEN s.term =3 THEN 0.0075
                                    WHEN s.term =5 THEN 0.01
                                    WHEN s.term = 7 THEN 0.0075
                                    WHEN s.term =10 THEN 0.0150
                                    END) as total from `farmers.combined_gl` c
                                    LEFT JOIN `farmers.seriatim_new` s ON s.mpolicy = c.policy_id AND s.set_month = c.set_month
                                    WHERE s.set_month ='{set_month}' AND account_number IN ("1-C00-4000-101","1-C00-4001-101") AND description IN ('Cancellation', 'Premium Income')
                '''
    else:
        #Initial ceded for Farmers MYGA. Make sure to run and update term field on farmers.combined_gl before running!.
        query = f'''select SUM(diff*quota_share*0.0275) as total from `farmers.combined_gl`
        WHERE term IN (select distinct plan from `farmers_fia.seriatim` WHERE converge_qs >0) AND description IN ('Cancellation', 'Premium Income') AND account_number IN ('1-C00-4000-102', '1-C00-4001-102') AND set_month ='{set_month}' and quota_share >0
        '''
    print(query)
    job = client.query(query)
    row = next(job.result(), None)
    total_value = row.total if row and row.total is not None else 0
    result_row = {"product": product, "fieldname": "cede", "total": total_value}
    result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
    print(result_row)


def run_reconciliation(set_month, product_filter=None):
    global result_df
    result_df = pd.DataFrame()

    all_products = ["farmers", "farmers_fia"]
    products = [product_filter.strip()] if product_filter else all_products

    withdrawal_types = ["full_surrender", "premium", "partial_surrender", "rmd", "other"]
    other_type = ["death_claims", "cancellation"]
    for product in products:
        for field in other_type:
                if product =='farmers_fia' and field =='cancellation':
                    continue
                other(set_month, field, product)
                print('-----------------------------------------------')

    for product in products:
        for field in withdrawal_types:
                withdrawals(set_month, field, product)
                print('-----------------------------------------------')
    account_number_product = {
        '1-C00-5100-101': 'farmers',
        '1-C00-4650-101': 'farmers',
        '1-C00-5100-102': 'farmers_fia',
        '1-C00-4650-102': 'farmers_fia',
    }

    for account_number, product in account_number_product.items():
        if product in products:
            charges(set_month, account_number)
            print('-----------------------------------------------')

    if 'farmers' in products:
        calc_expense(set_month)
        print('-----------------------------------------------')

    for product in products:
        admin_expense(product, set_month)

    for product in products:
        calc_cede(set_month, product)
        print('-----------------------------------------------')

    result_df.sort_values(by='product', inplace=True)

    result_df.to_excel('Query Results/Reconciliation/Farmers_reconciliation'+set_month+'.xlsx', index=False)

