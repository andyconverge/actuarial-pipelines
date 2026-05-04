import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta


def get_previous_month(set_month):
    # Parse the set_month string into a datetime object
    date = datetime.strptime(set_month, "%Y%m")

    # Subtract one month
    previous_month_date = date - relativedelta(months=1)

    # Format the result back into the "YYYYMM" string format
    previous_month = previous_month_date.strftime("%Y%m")

    return previous_month


def create_query(set_month, product):

    beginning_month = get_previous_month(set_month)

    query = f'''
    #SILAC AVRF Query 
    #-------------------------------------------------------------------------------------------#
    
    
    #Combination of 3 queries
    #Query produce these results
     -- Policies in "beginning month" with inflow, outflow from "month" and filtered by policy_type
     -- New issued policies in "month"
     -- Internal Reissues or Freelooks that don't exists in seriatim but in withdrawal and premium tables. These policies fetched from policy table
    
    
    DECLARE beginning_month STRING DEFAULT "{beginning_month}";
    
    DECLARE month STRING DEFAULT "{set_month}";
    
    DECLARE policy_type STRING DEFAULT '{product}';
    
    
    SELECT DISTINCT
      sv.policynumber as policy_number,
      CONCAT(p.issueyear, '-', p.issuemonth, '-', p.issueday) as issued_date,
      sv.converge,
      IFNULL(sv.totalpolicyav * sv.converge,0) as beg_av_qs,
      IFNULL(CASE WHEN (SELECT max(totalpolicypremiumbonus)
        FROM `denali.seriatim_values` 
        WHERE policynumber = sv.policynumber AND
        set_month = month) <  sv.totalpolicypremiumbonus THEN 0
        ELSE ( (SELECT max(totalpolicypremiumbonus*converge)
        FROM `denali.seriatim_values` 
        WHERE policynumber = sv.policynumber AND
        set_month = month) - (sv.totalpolicypremiumbonus * sv.converge))
        END,0)
      AS difference_premium_bonus,
    
      IFNULL( 
        ( 
          SELECT SUM(p.totalinitpremium) FROM `denali.premiums` p 
          WHERE p.premiumrecognitionyyyymm = CAST(month as INT64) and p.policynumber = sv.policynumber 
          AND (p.substatus is NULL or p.totalinitpremium >0)
          GROUP BY p.policynumber, p.substatus
        ) * converge,0) 
      AS total_init_prem_qs,
    
    
      IFNULL( 
        ( 
          SELECT SUM(p.totaladdtlpremium) FROM `denali.premiums` p 
          WHERE p.premiumrecognitionyyyymm = CAST(month as INT64) and p.policynumber = sv.policynumber 
          AND (p.substatus is NULL or p.totaladdtlpremium >0)
          GROUP BY p.policynumber, p.substatus
      )   * converge,0) 
      AS total_addtl_prem_qs,
    
      IFNULL((SELECT MAX(mtd_fixed_interest) FROM `denali.seriatim_values`
      WHERE set_month = month and sv.policynumber = policynumber
      )*converge,0)
      AS fixed_interest_qs,
    
    
      IFNULL((SELECT MAX(mtd_index_interest) FROM `denali.seriatim_values`
      WHERE set_month = month and sv.policynumber = policynumber
      )*converge,0)
      AS index_interest_qs, 
    
      IFNULL((SELECT totaldeath FROM `denali.deaths` d 
      WHERE d.set_month = month and sv.policynumber = d.policynumber) * converge,0)
      AS qs_db,
    
    
      IFNULL((SELECT sum(w.fullsurrenders) FROM `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS full_surrender_qs,
    
      IFNULL((SELECT sum(w.partialwithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS partial_withdrawal_surrender_qs,
    
      IFNULL((SELECT sum(w.cancellationamount) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS cancellation_qs,
    
      IFNULL((SELECT sum(a.totalannuitization) from `denali.annuitization` a
      WHERE a.policynumber = sv.policynumber and a.set_month = month
      GROUP by a.policynumber )*converge,0 ) 
      AS annuitization_qs,
    
      IFNULL((SELECT sum(w.rmdwithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS rmdwithdrawals_qs,
    
    
      IFNULL((SELECT sum(w.otherwithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month 
      GROUP by w.policynumber )*converge,0 ) 
      AS other_qs,
    
    
      IFNULL((SELECT sum(w.lifetimewithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS lifetime_qs,
    
    
      IFNULL((SELECT sum(w.homecare) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS homecare_qs,
    
    
      IFNULL((SELECT sum(w.nursinghome) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS nursing_care_qs,
    
    
      IFNULL((SELECT sum(w.terminalillness) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS terminal_illness_qs,
    
    
      IFNULL((SELECT sum(w.wellnesswithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS wellness_qs,
    
    
      IFNULL((SELECT sum(w.premiumtaxes) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS premium_taxes_qs,
    
      IFNULL((SELECT sum(w.deathdeferral) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS death_deferral_qs,
    
      IFNULL((SELECT sum(w.surrendercharges_includesmvaandbonusrecapture_) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month
      GROUP by w.policynumber )*converge,0 ) 
      AS surrender_charges_qs,
    
      CASE WHEN (
        SELECT max(totalpolicyav)
        FROM `denali.seriatim_values` 
        WHERE policynumber = sv.policynumber AND
        set_month = month
      ) is NULL then 0
      ELSE (
        SELECT max(totalpolicyav)
        FROM `denali.seriatim_values` 
        WHERE policynumber = sv.policynumber AND
        set_month = month
      )*converge
      END as end_av_qs,
    
      IFNULL((SELECT sum(w.internalreissues) from `denali.withdrawals` w
      WHERE w.policynumber = sv.policynumber and w.set_month = month 
      GROUP by w.policynumber )*sv.converge,0 ) 
      AS internal_reissues_qs,
    
      IFNULL((SELECT sum(reserves2) from `denali.seriatim_values` 
      WHERE policynumber = sv.policynumber and set_month = beginning_month 
      GROUP by policynumber )*sv.converge,0 )
      AS beginning_reserve_sum_qs,
    
      IFNULL((SELECT sum(reserves2) from `denali.seriatim_values` 
      WHERE policynumber = sv.policynumber and set_month = month 
      GROUP by policynumber )*sv.converge,0 )
      AS end_reserve_sum_qs,
    
      "0" as new_policy_check,
    
      CASE WHEN sv.policynumber 
        NOT IN 
          (SELECT policynumber FROM `denali.seriatim_values` 
          WHERE set_month = month and policynumber like policy_type) 
      THEN "1"
      ELSE "0" 
      END AS dropped_policy_check
    
    FROM `denali.seriatim_values` sv
    JOIN `denali.policy` p on p.policynumber = sv.policynumber and p.set_month = sv.set_month and p.creditstrategy = sv.creditstrategy
    WHERE sv.set_month = beginning_month AND sv.policynumber LIKE policy_type
    
    UNION ALL
    
    #New policies for month
    
    SELECT DISTINCT sv1.policynumber as policy_number,
      CONCAT(p1.issueyear, '-', p1.issuemonth, '-', p1.issueday) AS issued_date,
      sv1.converge,
    
      0 as beg_av_qs,
    
      sv1.totalpolicypremiumbonus * sv1.converge as difference_premium_bonus,
    
      IFNULL( 
        ( 
          select sum(p.totalinitpremium) from `denali.premiums` p 
          WHERE p.premiumrecognitionyyyymm = CAST(month as INT64) and p.policynumber = sv1.policynumber 
          AND (p.substatus is NULL or p.totalinitpremium >0)
          GROUP BY p.policynumber, p.substatus
        )* sv1.converge,0) 
      AS total_init_prem_qs,
    
      IFNULL( 
        ( 
          SELECT sum(p.totaladdtlpremium) FROM `denali.premiums` p 
          WHERE p.premiumrecognitionyyyymm = CAST(month as INT64) and p.policynumber = sv1.policynumber 
          AND (p.substatus is NULL or p.totaladdtlpremium >0)
          GROUP BY p.policynumber, p.substatus
          )* sv1.converge,0) 
      AS total_addtl_prem_qs,
    
      IFNULL((select MAX(mtd_fixed_interest) FROM `denali.seriatim_values`
      WHERE set_month = month and sv1.policynumber = policynumber
      )*sv1.converge,0) 
      AS fixed_interest_qs,
    
      IFNULL((select MAX(mtd_index_interest) FROM `denali.seriatim_values`
      WHERE set_month = month and sv1.policynumber = policynumber
      )*sv1.converge,0)
      AS index_interest_qs,
    
      IFNULL((select totaldeath from `denali.deaths` d 
      WHERE d.set_month = month and sv1.policynumber = d.policynumber) * sv1.converge ,0)
      AS qs_db,
    
      IFNULL((SELECT sum(w.fullsurrenders) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS full_surrender_qs,
    
      IFNULL((SELECT sum(w.partialwithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS partial_withdrawal_surrender_qs,
    
      IFNULL((SELECT sum(w.cancellationamount) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS cancellation_qs,
    
      IFNULL((SELECT sum(a.totalannuitization) from `denali.annuitization` a
      WHERE a.policynumber = sv1.policynumber and a.set_month = month
      GROUP by a.policynumber )*sv1.converge,0 ) 
      AS annuitization_qs,
    
      IFNULL((SELECT sum(w.rmdwithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS rmdwithdrawals_qs,
    
    
      IFNULL((SELECT sum(w.otherwithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge, 0 ) 
      AS other_qs,
    
    
      IFNULL((SELECT sum(w.lifetimewithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge, 0 ) 
      AS lifetime_qs,
    
    
      IFNULL((SELECT sum(w.homecare) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS homecare_qs,
    
    
      IFNULL((SELECT sum(w.nursinghome) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS nursing_care_qs,
    
    
      IFNULL((SELECT sum(w.terminalillness) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS terminal_illness_qs,
    
    
      IFNULL((SELECT sum(w.wellnesswithdrawals) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS wellness_qs,
    
    
      IFNULL((SELECT sum(w.premiumtaxes) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS premium_taxes_qs,
      
      
      IFNULL((SELECT sum(w.deathdeferral) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS death_deferral_qs,
    
      IFNULL((SELECT sum(w.surrendercharges_includesmvaandbonusrecapture_) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month 
      GROUP by w.policynumber )*sv1.converge,0 ) 
      AS surrender_charges_qs,
    
      IFNULL((
        SELECT max(totalpolicyav) FROM `denali.seriatim_values` 
        WHERE policynumber = sv1.policynumber AND set_month = month
      ) *sv1.converge,0 ) 
      AS end_av_qs,
    
      IFNULL((SELECT sum(w.internalreissues) from `denali.withdrawals` w
      WHERE w.policynumber = sv1.policynumber and w.set_month = month 
      GROUP by w.policynumber )*sv1.converge ,0) 
      AS internal_reissues_qs,
    
      IFNULL((SELECT sum(reserves2) from `denali.seriatim_values` 
      WHERE policynumber = sv1.policynumber and set_month = beginning_month 
      GROUP by policynumber )*sv1.converge, 0)
      AS beginning_reserve_sum_qs,
    
      IFNULL((SELECT sum(reserves2) from `denali.seriatim_values` 
      WHERE policynumber = sv1.policynumber and set_month = month 
      GROUP by policynumber )*sv1.converge,0 )
      AS end_reserve_sum_qs,
    
    
      "1" as new_policy_check,
      "0" as dropped_policy_check
    
    
    FROM `denali.seriatim_values` sv1
    JOIN `denali.policy` p1 ON sv1.policynumber = p1.policynumber and sv1.set_month = p1.set_month 
      AND sv1.creditstrategy = p1.creditstrategy
    WHERE sv1.set_month = month AND 
    sv1.policynumber NOT IN 
              (select policynumber from `denali.seriatim_values` where set_month = beginning_month) 
    AND sv1.policynumber like policy_type
    
    UNION ALL
    
    SELECT p3.policynumber as policy_number, 
    CONCAT(p3.issueyear, '-', p3.issuemonth, '-', p3.issueday) AS issued_date,
    (SELECT max(converge) FROM `denali.lookup` WHERE p3.reinsurancecode = reins) as converge,
    0 as beg_av_qs,
    0 as difference_premium_bonus,
      IFNULL(( 
        SELECT 
         CASE WHEN p.substatus IS NOT NULL THEN 0
          ELSE SUM(p.totalinitpremium)
          END as test
        FROM `denali.premiums` p 
        WHERE p.premiumrecognitionyyyymm = CAST(month as INT64) AND p.policynumber = p3.policynumber 
        AND (p.substatus is NULL or p.totaladdtlpremium >0)
        GROUP BY p.policynumber, p.substatus
      ) * (SELECT max(converge) from `denali.lookup` WHERE p3.reinsurancecode = reins),0
      ) AS total_init_prem_qs,
      IFNULL(( 
        SELECT 
         CASE WHEN p.substatus IS NOT NULL THEN 0
          ELSE SUM(p.totaladdtlpremium)
          END as test
        FROM `denali.premiums` p 
        WHERE p.premiumrecognitionyyyymm = CAST(month as INT64) AND  p.policynumber = p3.policynumber 
        AND (p.substatus is NULL OR p.totalinitpremium >0)
        GROUP BY p.policynumber, p.substatus
      ) * (SELECT max(converge) from `denali.lookup` WHERE p3.reinsurancecode = reins),0
      ) as total_addtl_prem_qs,
      0 as fixed_interest_without_qs, 0 as index_interest_qs,0 as qs_db,
      0 as full_surrender_qs, 0 as partial_withdrawal_surrender_qs,
    
      IFNULL((SELECT sum(w.cancellationamount) FROM `denali.withdrawals` w
      WHERE w.policynumber = p3.policynumber and w.set_month = month
      GROUP BY w.policynumber )*(SELECT max(converge) from `denali.lookup` WHERE p3.reinsurancecode = reins),0 ) 
      AS cancellation_qs,
    
      0 as annuitization_qs, 0 as rmdwithdrawals_qs, 0 as other_qs, 0 as lifetime_qs,
      0 as homecare_qs, 0 as nursing_care_qs, 0 as terminal_illness_qs, 0 as wellness_qs,
      0 as premium_taxes_qs,
    
      0 as death_deferral_qs,
      IFNULL((SELECT sum(w.surrendercharges_includesmvaandbonusrecapture_) FROM `denali.withdrawals` w
      WHERE w.policynumber = p3.policynumber and w.set_month = month
      GROUP by w.policynumber )*(SELECT max(converge) from `denali.lookup` WHERE p3.reinsurancecode = reins),0 ) 
      AS surrender_charges_qs,
      
      0 as end_av_qs,
    
      IFNULL((SELECT sum(w.internalreissues) from `denali.withdrawals` w
      WHERE w.policynumber = p3.policynumber and w.set_month = month
      GROUP by w.policynumber )*(SELECT max(converge) from `denali.lookup` WHERE p3.reinsurancecode = reins),0 ) 
      AS internal_reissues_qs,
      IFNULL((SELECT sum(reserves2) from `denali.seriatim_values` 
      WHERE policynumber = p3.policynumber and set_month = beginning_month 
      GROUP by policynumber )*(SELECT max(converge) from `denali.lookup` WHERE p3.reinsurancecode = reins),0 )
      AS beginning_reserve_sum_qs,
    
      IFNULL((SELECT sum(reserves2) from `denali.seriatim_values` 
      WHERE policynumber = p3.policynumber and set_month = month 
      GROUP by policynumber )*(SELECT max(converge) from `denali.lookup` WHERE p3.reinsurancecode = reins),0 )
      AS end_reserve_sum_qs,
      "0" AS new_policy_check, 
      "1" AS dropped_policy_check
    FROM `denali.policy` p3
    WHERE p3.policynumber like policy_type AND p3.set_month = month AND 
    p3.policynumber NOT IN (SELECT policynumber FROM `denali.seriatim_values`)
    
    
    ORDER BY policy_number;
    '''
    return query


def run_avrf_analysis(set_month, product):
    print('check set_month value:', set_month)

    CREDS = '../../converge-database-0331482f2ee5.json'

    # Initialize BigQuery client using the credentials dictionary
    client = bigquery.Client.from_service_account_info(CREDS)

    # Test client connection
    print("BigQuery client initialized successfully!")
    print('Running AVRF analysis for set_month: \n', set_month, product)
    threshold = 10000
    query_result = create_query(set_month, product)
    result = client.query(query_result)

    avrf_result = pd.DataFrame()

    # Define the column headers based on the provided mapping
    columns = ['policy_number', 'issued_date', 'converge', 'beg_av_qs', 'difference_premium_bonus',
               'total_init_prem_qs', 'total_addtl_prem_qs', 'fixed_interest_qs', 'index_interest_qs',
               'qs_db', 'full_surrender_qs', 'partial_withdrawal_surrender_qs', 'cancellation_qs',
               'annuitization_qs', 'rmdwithdrawals_qs', 'other_qs', 'lifetime_qs', 'homecare_qs',
               'nursing_care_qs', 'terminal_illness_qs', 'wellness_qs', 'premium_taxes_qs','death_deferral_qs',
               'surrender_charges_qs','end_av_qs', 'internal_reissues_qs', 'beginning_reserve_sum_qs',
               'end_reserve_sum_qs', 'new_policy_check', 'dropped_policy_check']

    # Extract the rows from the query result and convert it to a list of tuples
    data = [tuple(row) for row in result]  # Assuming each row is iterable

    # Create a DataFrame from the data and column headers
    avrf_result = pd.DataFrame(data, columns=columns)

    avrf_result['inflow'] = avrf_result['fixed_interest_qs']+avrf_result['index_interest_qs'] + \
        avrf_result['total_init_prem_qs']+avrf_result['total_addtl_prem_qs']
    avrf_result['outflow'] = avrf_result.iloc[:, 9:23].sum(axis=1)
    avrf_result['exp_av'] = avrf_result['beg_av_qs'] + \
        avrf_result['inflow']-avrf_result['outflow']
    avrf_result['diff'] = avrf_result['end_av_qs']-avrf_result['exp_av']
    # substract inflow

    sum_diff = avrf_result['diff'].abs().sum()

    if sum_diff > threshold:
        print('difference is more then threshold amount:')

    avrf_result.to_excel('AVRF_'+str(set_month)+product+'.xlsx', index=False)

    print('AVRF result exported and Complete')
