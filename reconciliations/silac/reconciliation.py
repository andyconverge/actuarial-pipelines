import pandas as pd
import csv
import pandas_gbq
import time
from datetime import datetime
import os
from google.oauth2 import service_account
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta

#credentials and client setup
CREDS = '../converge-database-0331482f2ee5.json'
# Initialize an empty results DataFrame to collect reconciliation rows
result_df = pd.DataFrame(columns=['product', 'fieldname', 'total'])
client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)

policynumber_type = ["D%","T%"]
withdrawal_types = ['internalreissues','fullsurrenders', 'partialwithdrawals', 'cancellationamount', 'rmdwithdrawals', 'otherwithdrawals', 'lifetimewithdrawals', 'homecare', 'nursinghome',  'terminalillness', 'wellnesswithdrawals','surrendercharges_includesmvaandbonusrecapture_', 'premiumtaxes']


#Parse date function
def transform_date(yyyymm: str) -> str:
    # Parse 'YYYYMM' format into a pandas Timestamp
    date = pd.to_datetime(yyyymm, format='%Y%m')
    
    # Get the last day of that month
    last_day = date + pd.offsets.MonthEnd(0)
    
    # Return formatted string
    return last_day.strftime('%Y-%m-%d')

#gets previous month in 'YYYYMM' format

def get_previous_month(set_month):
    # Parse the set_month string into a datetime object
    date = datetime.strptime(set_month, "%Y%m")

    # Subtract one month
    previous_month_date = date - relativedelta(months=1)

    # Format the result back into the "YYYYMM" string format
    previous_month = previous_month_date.strftime("%Y%m")

    return previous_month


#Query to product withdrawal results

def query_function(set_month, policynumber_type, withdrawal_type):
  global result_df

  query = 'SELECT CAST(sum(w.'+withdrawal_type+'*(SELECT max(sv.converge) FROM `denali.seriatim_values` AS sv \
  WHERE sv.policynumber = w.policynumber group by w.policynumber) ) as INT) as '+withdrawal_type+'_sum\
  FROM `denali.withdrawals` w WHERE set_month = "'+set_month+'" AND w.policynumber LIKE "'+  policynumber_type +'"'
  print(query)
  job = client.query(query)
  for j in job.result():
    result_row = {
      'product': 'Denali' if policynumber_type == 'D%' else 'Teton',
      'fieldname': withdrawal_type,
      'total': j[0],
    }
    result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
    print(result_row)

def query_request(query, fieldname, set_month, policynumber_type):
    job = client.query(query)
    for j in job.result():
        result_row = {
            'product': 'Denali' if policynumber_type == 'D%' else 'Teton',
            'fieldname': fieldname,
            'total': j[0],
        }
    return result_row

#Gross Initial Premium and Gross Additional Premium
def premiums(set_month, policynumber_type):
  global result_df

  premium_query = 'WITH initial_prem AS (\
  SELECT pm.policynumber as policynumber, pm.totalinitpremium*(SELECT max(sv.converge) from `denali.seriatim_values` sv WHERE sv.policynumber = pm.policynumber) as gross_intial, from `denali.premiums` pm\
  where pm.premiumrecognitionyyyymm = CAST('+set_month+' AS INT64) and pm.policynumber like "'+policynumber_type+'" and pm.substatus is NULL and pm.totalinitpremium IS NOT NULL) \
  SELECT SUM(gross_intial) from initial_prem'
  print(premium_query)
  job = client.query(premium_query)
  for j in job.result():
    result_row = {
      'product': 'Denali' if policynumber_type == 'D%' else 'Teton',
      'fieldname': 'gross_initial_premium',
      'total' : j[0],
    }
    gross_init_prem = j[0]
    result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
    print(result_row)
  adttl_prem = 'WITH addtl_prem AS (\
  SELECT pm.policynumber as policynumber, pm.totaladdtlpremium*(SELECT max(sv.converge) from `denali.seriatim_values` sv WHERE sv.policynumber = pm.policynumber) as gross_intial, from `denali.premiums` pm\
  where pm.premiumrecognitionyyyymm = CAST('+set_month +' AS INT64) and pm.policynumber like "'+policynumber_type+'" and pm.substatus is NULL and pm.totaladdtlpremium IS NOT NULL) \
  SELECT SUM(gross_intial) from addtl_prem'
  print(adttl_prem)
  job = client.query(adttl_prem)
  for j in job.result():
    result_row = {
      'product': 'Denali' if policynumber_type == 'D%' else 'Teton',
      'fieldname': 'gross_addtl_premium',
      'total' : j[0],
    }
    gross_addtl_prem = j[0]
    result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
    print(result_row)

#Commision query run function
def commission(set_month, policynumber_type):
  global result_df

  commission_query = 'SELECT SUM(commissionamount*(SELECT max(sv.converge) from `denali.seriatim_values` sv WHERE sv.policynumber = c.policynumber)) from `denali.commissions` c \
            WHERE c.commissionamount > 0 AND commissionrecognitionyyyymm = CAST('+set_month+' as INT64) AND policynumber LIKE "'+  policynumber_type +'"'
  job = client.query(commission_query)
  for j in job.result():
    result_row = {
      'product': 'Denali' if policynumber_type == 'D%' else 'Teton',
      'fieldname': 'initial_commission_paid',
      'total' : j[0],
    }
    result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
    print(result_row)
  commission_negative = 'SELECT SUM(commissionamount*(SELECT max(sv.converge) from `denali.seriatim_values` sv WHERE sv.policynumber = c.policynumber)) from `denali.commissions` c \
            WHERE c.commissionamount < 0 AND commissionrecognitionyyyymm = CAST('+set_month+' AS INT64) AND policynumber LIKE "'+  policynumber_type +'"' 
  job = client.query(commission_negative)
  for j in job.result():
    result_row = {
      'product': 'Denali' if policynumber_type == 'D%' else 'Teton',
      'fieldname': 'commission_recovered',
      'total' : j[0],
    }
    result_df = pd.concat([result_df, pd.DataFrame([result_row])], ignore_index=True)
    print(result_row)

#Death amount query run function
def death_amount(set_month, policynumber_type):
  global result_df

  death_query = 'SELECT CAST(sum(d.totaldeath*(SELECT max(sv.converge) FROM `denali.seriatim_values` AS sv \
  WHERE sv.policynumber = d.policynumber group by d.policynumber) ) as INT) as death_sum\
  FROM `denali.deaths` d WHERE set_month = "'+set_month+'" AND d.policynumber LIKE "'+  policynumber_type +'"'
  print(death_query)
  death_job = client.query(death_query)
  for j in death_job.result():
    death_row = {
      'product': 'Denali' if policynumber_type == 'D%' else 'Teton',
      'fieldname': 'death',
      'total': j[0],
    }
    result_df = pd.concat([result_df, pd.DataFrame([death_row])], ignore_index=True)
    print(death_row)


# Expense query run function

def part_h(set_month: str, policynumber_type: str):
    """
    set_month: 'YYYYMM' string, e.g., '202508'
    policynumber_type: pattern for LIKE, e.g., 'A%' or '%'
    """
    global result_df

    # 1) Acquisition Expenses - No. of contracts issued
    query = f"""
    WITH new_policy AS (
      SELECT DISTINCT p.policynumber
      FROM `denali.policy` p
      JOIN `denali.seriatim_values` sv ON sv.policynumber = p.policynumber
      WHERE issueyear = 2023
        AND issuemonth > 4
        AND sv.converge > 0
    )
    SELECT COUNT(DISTINCT policynumber)
    FROM `denali.premiums`
    WHERE premiumrecognitionyyyymm = CAST("{set_month}" AS INT64)
      AND policynumber IN (SELECT policynumber FROM new_policy)
      AND policynumber LIKE "{policynumber_type}"
    """
    result_df = pd.concat(
        [result_df, pd.DataFrame([query_request(query, 'number_of_contract_issued_acq', set_month, policynumber_type)])],
        ignore_index=True
    )

    # 2) Acquisition Expense ($100 per Policy x No. of Contracts Issued)
    query = f"""
    WITH new_policy AS (
      SELECT DISTINCT p.policynumber
      FROM `denali.policy` p
      JOIN `denali.seriatim_values` sv ON sv.policynumber = p.policynumber
      WHERE issueyear = 2023
        AND issuemonth > 4
        AND sv.converge > 0
    ),
    prem_list_new AS (
      SELECT
        p.policynumber,
        (SELECT MAX(sv.converge) FROM `denali.seriatim_values` sv WHERE sv.policynumber = p.policynumber) AS qs
      FROM `denali.premiums` p
      WHERE premiumrecognitionyyyymm = CAST("{set_month}" AS INT64)
        AND p.policynumber IN (SELECT policynumber FROM new_policy)
        AND p.policynumber LIKE "{policynumber_type}"
    )
    SELECT COUNT(DISTINCT policynumber) * AVG(qs) * 100
    FROM prem_list_new
    """
    result_df = pd.concat(
        [result_df, pd.DataFrame([query_request(query, 'acq_expense ($100 per policy x #contracts issued)', set_month, policynumber_type)])],
        ignore_index=True
    )

    # 3) Maintenance Expenses ($60 per year x No. of Contracts Inforce (2% Inflation Adjustment))
    #    Note: keeping your original rec formula; just fixed syntax and references.
    query = f"""
    WITH maint_expense AS (
      SELECT DISTINCT
        sv.policynumber,
        sv.converge AS qs,
        l.maintenance AS maintenance,
        (
          POWER(
            1.02,
            FLOOR(
              DATE_DIFF(
                DATE("{transform_date(set_month)}"),
                CAST(CONCAT(p.issueyear, "-", p.issuemonth, "-", p.issueday) AS DATE),
                YEAR
              )
            ) - 1
          ) / 12
        ) AS rec
      FROM `denali.seriatim_values` sv
      JOIN `denali.lookup` l ON sv.reinsurancecode = l.reins
      JOIN (
        SELECT DISTINCT policynumber, issueyear, issuemonth, issueday
        FROM `denali.policy`
      ) p ON p.policynumber = sv.policynumber
      WHERE sv.set_month = "{set_month}"
        AND sv.policynumber LIKE "{policynumber_type}"
    )
    SELECT SUM(maintenance * qs * rec)
    FROM maint_expense
    """
    result_df = pd.concat(
        [result_df, pd.DataFrame([query_request(query, 'maintance_expense', set_month, policynumber_type)])],
        ignore_index=True
    )

    # 4) Hedging Costs
    query = f"""
    SELECT SUM(
      (n.initallocamount + n.reallocamount) *
      (SELECT MAX(sv.optionbudget) FROM `denali.seriatim_values` sv WHERE sv.policynumber = n.policynumber and set_month = "{get_previous_month(set_month)}") *
      (SELECT MAX(sv.converge) FROM `denali.seriatim_values` sv WHERE sv.policynumber = n.policynumber)
    )
    FROM `denali.notional` n
    WHERE n.creditingstrategy <> "FI"
      AND FORMAT_TIMESTAMP("%Y%m", trandate) = "{set_month}"
      AND n.policynumber LIKE "{policynumber_type}"
    """
    result_df = pd.concat(
        [result_df, pd.DataFrame([query_request(query, 'hedgin_cost', set_month, policynumber_type)])],
        ignore_index=True
    )

    # 5) Option Payoff
    query = f"""
    WITH seriatim_distinct AS (
      SELECT DISTINCT policynumber, mtd_index_interest, converge
      FROM `denali.seriatim_values`
      WHERE set_month = "{set_month}"
        AND policynumber LIKE "{policynumber_type}"
    )
    SELECT -SUM(mtd_index_interest * converge)
    FROM seriatim_distinct
    """
    result_df = pd.concat(
        [result_df, pd.DataFrame([query_request(query, 'option_payoff', set_month, policynumber_type)])],
        ignore_index=True
    )

    # 6) Ceding Allowance at Issue
    query = f"""
    SELECT SUM(
      (CASE WHEN p.substatus IS NOT NULL AND p.totalinitpremium < 0 THEN 0 ELSE p.totalinitpremium END
       + CASE WHEN p.substatus IS NOT NULL AND p.totaladdtlpremium < 0 THEN 0 ELSE p.totaladdtlpremium END) *
      (
        SELECT MAX(l.converge) * MAX(l.cede)
        FROM `denali.lookup` l
        JOIN `denali.policy` pl
          ON pl.reinsurancecode = l.reins
         AND p.policynumber = pl.policynumber
      )
    )
    FROM `denali.premiums` p
    WHERE p.premiumrecognitionyyyymm = CAST("{set_month}" AS INT64)
      AND p.policynumber LIKE "{policynumber_type}"
    """
    result_df = pd.concat(
        [result_df, pd.DataFrame([query_request(query, 'ceding_allowance', set_month, policynumber_type)])],
        ignore_index=True
    )

    # 7) Refunded ceding allowance from cancellations
    query = f"""
    WITH refunded_list AS (
      SELECT
        w.cancellationamount,
        w.internalreissues,
        (
          SELECT MAX(l.converge) * MAX(l.cede)
          FROM `denali.lookup` l
          JOIN `denali.policy` pl
            ON pl.reinsurancecode = l.reins
           AND w.policynumber = pl.policynumber
        ) AS ceded_qs
      FROM `denali.withdrawals` w
      WHERE w.set_month = "{set_month}"
        AND w.policynumber LIKE "{policynumber_type}"
    )
    SELECT SUM(cancellationamount * ceded_qs) + SUM(internalreissues * ceded_qs)
    FROM refunded_list
    """
    result_df = pd.concat(
        [result_df, pd.DataFrame([query_request(query, 'refunded_ceding_allowance', set_month, policynumber_type)])],
        ignore_index=True
    )

    # 8) Increase due to premium bonus
    prev_month = get_previous_month(set_month)  # expects 'YYYYMM'
    query = f"""
    WITH prem_bonus_diff AS (
      SELECT DISTINCT
        sv.policynumber,
        sv.totalpolicypremiumbonus,
        (
          SELECT MAX(totalpolicypremiumbonus)
          FROM `denali.seriatim_values`
          WHERE set_month = "{prev_month}"
            AND policynumber = sv.policynumber
        ) AS starting_bonus,
        sv.converge
      FROM `denali.seriatim_values` sv
      WHERE sv.set_month = "{set_month}"
        AND sv.policynumber LIKE "{policynumber_type}"
    )
    SELECT SUM( (totalpolicypremiumbonus - starting_bonus) * converge )
    FROM prem_bonus_diff
    """
    result_df = pd.concat(
        [result_df, pd.DataFrame([query_request(query, 'increase_due_to_prem_bonus', set_month, policynumber_type)])],
        ignore_index=True
    )

    # 9) Increase due to Fixed Interest
    query = f"""
    WITH seriatim_distinct AS (
      SELECT DISTINCT policynumber, mtd_fixed_interest, converge
      FROM `denali.seriatim_values`
      WHERE set_month = "{set_month}"
        AND policynumber LIKE "{policynumber_type}"
    )
    SELECT -SUM(mtd_fixed_interest * converge)
    FROM seriatim_distinct
    """
    result_df = pd.concat(
        [result_df, pd.DataFrame([query_request(query, 'increase_due_fixed_interest', set_month, policynumber_type)])],
        ignore_index=True
    )

def run_reconciliation(set_month):
  global result_df
  # reinitialize so each run starts with empty results
  result_df = pd.DataFrame(columns=['product', 'fieldname', 'total'])

  print("Starting the program \n")
  print("Running reconciliation for SILAC", set_month)

  for product_type in policynumber_type:
    for field in withdrawal_types:
      query_function(set_month, product_type, field)
      print('-----------------------------------------------')
    premiums(set_month, product_type)
    death_amount(set_month, product_type)
    commission(set_month, product_type)
    part_h(set_month, product_type)
    print('-----------------------------------------------')
  result_df.to_excel('Results/Reconciliations/SILAC_reconciliation_result_'+set_month+'.xlsx', index=False)
  print('end')
  return result_df
