import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta

CREDS = '../converge-database-0331482f2ee5.json'
client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)

def get_previous_month(set_month):
    # Parse the set_month string into a datetime object
    date = datetime.strptime(set_month, "%Y%m")

    # Subtract one month
    previous_month_date = date - relativedelta(months=1)

    # Format the result back into the "YYYYMM" string format
    previous_month = previous_month_date.strftime("%Y%m")

    return previous_month

def parse_year_month(date_str):
    return datetime.strptime(date_str, "%Y%m").replace(day=1).date()

def create_query(month, product):
    if product =='myga':
        query = f'''
        #DECLARE month STRING DEFAULT "202508";

        #Part 1 inforce policies

        #STARTS HERE

        #-------------------------------------------------------------#

        WITH death_list as (
        select policy_number, (SELECT max(set_month) from `farmers.seriatim`  WHERE policy_number = d.policy_number GROUP by policy_number) as set_month, d.quotashare, net_amount from `farmers.death_claims` d WHERE set_month = "{month}"
        )

        SELECT 
        "{month}" as set_month,
        f.mpolicy as policy_number, 
        DATE(f.missdt) as issue_date,
        f.mage as issue_age,
        f.missuest as issue_state,
        IFNULL(f.mcurrbal*converge_qs,0) as beginning_av,
        IFNULL(f1.mcurrbal*converge_qs,0) as end_av,  
        IFNULL(f.converge_qs,0) as converge_qs,
        IFNULL(f1.int_diff*converge_qs,0) as int_in_current_month,
        IFNULL((SELECT SUM(gross_premium*quota_share) FROM `farmers.premium` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) as premium,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.rmd` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS ceded_RMD,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.partial_surrender` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS ceded_partial,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.other` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS ceded_free_wd,
        IFNULL((SELECT SUM(net_amount) FROM `farmers.cancellation` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS ceded_cancellation,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.full_surrender` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS full_surrenders,
        IFNULL((SELECT SUM(net_amount) FROM `farmers.death_claims` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS death_claim,
        IFNULL((SELECT SUM(diff * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f.mpolicy
            AND account_number = '1-C00-4650-101'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            ) 
            GROUP by policy_id ) ,0) as market_value_adjustment,
            IFNULL((SELECT SUM(diff * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f.mpolicy
            AND account_number = '1-C00-4650-101'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            )
            GROUP by policy_id ) ,0) as surrender_charge,

        0 as dropped_policy_check,
        0 as new_policy_check
        FROM `farmers.seriatim_new` f
        LEFT JOIN (
            SELECT 
            f1.mpolicy,
            f1.set_month,
            f1.mcurrbal,
            f1.int_diff,
            f1.mschg,
            f1.current_status,
            f1.spousal_cont
            FROM `farmers.seriatim_new` f1
            WHERE f1.set_month = "{month}"
        ) f1 ON f.mpolicy = f1.mpolicy
        WHERE f.set_month = "{get_previous_month(month)}" and f1.mpolicy is not NULL AND f1.current_status ='Active' 
        

        UNION ALL
        #new policies
        SELECT 
        f3.set_month,
        f3.mpolicy as policy_number, 
        DATE(f3.missdt) as issue_date,
        f3.mage as issue_age,
        f3.missuest as issue_state,
        0 as beginning_av,
        IFNULL(f3.mcurrbal*converge_qs,0) as end_av, 
        IFNULL(f3.converge_qs,0) as converge_qs,
        IFNULL(f3.mint*converge_qs,0) as int_in_current_month,
        IFNULL((SELECT SUM(gross_premium*quota_share) FROM `farmers.premium` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) as premium,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.rmd` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS ceded_RMD,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.partial_surrender` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS ceded_partial,
            
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.other` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS ceded_free_wd,
        IFNULL((SELECT SUM(net_amount) FROM `farmers.cancellation` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS ceded_cancellation,

        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.full_surrender` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS full_surrenders,
        0 AS death_claim,

        IFNULL((SELECT SUM(diff * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f3.mpolicy
            AND account_number = '1-C00-4650-101'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            ) 
            GROUP by policy_id ) ,0) as market_value_adjustment,
        IFNULL((SELECT SUM(diff * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f3.mpolicy
            AND account_number = '1-C00-4650-101'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            )
            GROUP by policy_id ) ,0) as surrender_charge,
        0 as dropped_policy_check,
        1 as new_policy_check
        FROM `farmers.seriatim_new` f3
        WHERE f3.set_month = "{month}" AND f3.mpolicy NOT IN (SELECT mpolicy FROM `farmers.seriatim_new` WHERE set_month = "{get_previous_month(month)}")

        UNION ALL

        #Death only from death list

        SELECT 
        f4.set_month,
        f4.policy_number, 
        (SELECT issue_date FROM `farmers.seriatim` WHERE policy_number = f4.policy_number and set_month = f4.set_month) as issue_date,
        (SELECT issue_age FROM `farmers.seriatim` WHERE policy_number = f4.policy_number and set_month = f4.set_month) as issue_age,
        (SELECT issue_state FROM `farmers.seriatim` WHERE policy_number = f4.policy_number and set_month = f4.set_month) as issue_state,
        IFNULL((SELECT gross_account_value*quota_share FROM `farmers.seriatim` WHERE policy_number = f4.policy_number and set_month = f4.set_month)*f4.quotashare,0) as beginning_av,
        0 as end_av,
        IFNULL(f4.quotashare,0) as converge_qs,
        0 as int_in_current_month,
        0 as premium,
        0 AS ceded_RMD,
        0 AS ceded_partial,
        0 AS ceded_free_wd,
        0 AS ceded_cancellation,
        0 as full_surrender,
        IFNULL(f4.net_amount*f4.quotashare,0) as death_claim,
        0 as market_value_adjustment,
        0 as surrender_charge,
        1 as dropped_policy_check,
        0 as new_policy_check
        FROM death_list as f4 WHERE f4.quotashare >0

        UNION ALL
        #DROPPED POLICIES STARTS HERE
        #-----------------------------------------------------------------------------------------------------------------------------------# only surrender and dropped policies   
        SELECT 
        "{month}" as set_month,
        f2.mpolicy as policy_number, 
        DATE(f2.missdt) as issue_date,
        f2.mage as issue_age,
        f2.missuest as issue_state,
        IFNULL(f2.mcurrbal*f2.converge_qs,0) as beginning_av,
        0 as end_av,  
        IFNULL(f2.converge_qs,0) as converge_qs,
        IFNULL(f2.int_diff*f2.converge_qs,0) as int_in_current_month,
        IFNULL((SELECT SUM(gross_premium*quota_share) FROM `farmers.premium` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) as premium,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.rmd` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS ceded_RMD,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.partial_surrender` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS ceded_partial,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.other` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS ceded_free_wd,
        IFNULL((SELECT SUM(net_amount) FROM `farmers.cancellation` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS ceded_cancellation,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers.full_surrender` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS full_surrenders,
        IFNULL((SELECT SUM(net_amount) FROM `farmers.death_claims` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS death_claim,
            IFNULL((SELECT SUM(diff * quota_share) AS total
                FROM `farmers.combined_gl`
                WHERE set_month = "{month}"
                AND policy_id = f2.mpolicy
                AND account_number = '1-C00-4650-101'
                AND description IN (
                    'Annuity Regular Distribution',
                    'Annuity RMD',
                    'Annuity Partial Surrender',
                    'Annuity Full Surrender'
                ) 
                GROUP by policy_id ) ,0) as market_value_adjustment,

            IFNULL((SELECT SUM(credit_amount * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f2.mpolicy
            AND account_number = '1-C00-4650-101'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            )
            GROUP by policy_id ) ,0) as surrender_charge,
        
        1 as dropped_policy_check,
        0 as new_policy_check
        FROM `farmers.seriatim_new` f2
        WHERE f2.current_status ='Active' AND f2.set_month = "{get_previous_month(month)}" 
        AND f2.mpolicy NOT IN (SELECT mpolicy FROM `farmers.seriatim_new` WHERE set_month = "{month}" AND current_status='Active') 
        and f2.mpolicy NOT IN (select policy_number FROM death_list) 
        AND f2.mpolicy NOT IN (select policy_number from `farmers.last_pending_claims` WHERE set_month = "{month}")


        ORDER BY policy_number;
        '''
    elif product =='fia':
        query = f'''
        
        ################################################
        #Re-useing the MYGA AVRF query.

        #DECLARE month STRING DEFAULT "202508";

        #Part 1 inforce policies

        #STARTS HERE

        #-------------------------------------------------------------#
        #current inforce policies takes all active policies from prev month and still active in current

        WITH death_list as (
        #policy claims are always delayed. This CTE query will find the latest inforce set_month for died policies,
        select policy_number, (SELECT max(set_month) from `farmers_fia.seriatim`  WHERE mpolicy = d.policy_number AND current_status='Active' GROUP by mpolicy) as set_month, d.quotashare, net_amount from `farmers_fia.death_claims` d WHERE set_month = "{month}"
        )

        SELECT 
        "{month}" as set_month,
        f.mpolicy as policy_number, 
        DATE(f.missdt) as issue_date,
        f.mage as issue_age,
        f.missuest as issue_state,
        IFNULL(f.mcurrbal*converge_qs,0) as beginning_av,
        IFNULL(f1.mcurrbal*converge_qs,0) as end_av,  
        IFNULL(f.converge_qs,0) as converge_qs,
        IFNULL(f1.int_diff*converge_qs,0) as int_in_current_month,
        IFNULL((SELECT SUM(gross_premium*quota_share) FROM `farmers_fia.premium` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) as premium,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.rmd` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS ceded_RMD,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.partial_surrender` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS ceded_partial,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.other` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS ceded_free_wd,
        IFNULL((SELECT SUM(net_withdrawal) FROM `farmers_fia.cancellation` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS ceded_cancellation,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.full_surrender` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS full_surrenders,
        IFNULL((SELECT SUM(net_amount) FROM `farmers_fia.death_claims` WHERE set_month = "{month}" AND f.mpolicy = policy_number),0) AS death_claim,
        IFNULL((SELECT SUM(diff * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f.mpolicy
            AND account_number = '1-C00-4650-101'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            ) 
            GROUP by policy_id ) ,0) as market_value_adjustment,
            IFNULL((SELECT SUM(credit_amount * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f.mpolicy
            AND account_number = '1-C00-4650-101'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            )
            GROUP by policy_id ) ,0) as surrender_charge,

        0 as dropped_policy_check,
        0 as new_policy_check
        FROM `farmers_fia.seriatim` f
        LEFT JOIN (
            SELECT 
            f1.mpolicy,
            f1.set_month,
            f1.mcurrbal,
            f1.int_diff,
            f1.mschg,
            f1.current_status,
            f1.spousal_cont
            FROM `farmers_fia.seriatim` f1
            WHERE f1.set_month = "{month}"
        ) f1 ON f.mpolicy = f1.mpolicy
        WHERE f.set_month = "{get_previous_month(month)}" AND f1.current_status ='Active' AND f.converge_qs >0

        UNION ALL
        #new policies checks by listing policies exists current month don't exist prev month
        SELECT 
        f3.set_month,
        f3.mpolicy as policy_number, 
        DATE(f3.missdt) as issue_date,
        f3.mage as issue_age,
        f3.missuest as issue_state,
        0 as beginning_av,
        IFNULL(f3.mcurrbal*converge_qs,0) as end_av, 
        IFNULL(f3.converge_qs,0) as converge_qs,
        IFNULL(f3.mint*converge_qs,0) as int_in_current_month,
        IFNULL((SELECT SUM(gross_premium*quota_share) FROM `farmers_fia.premium` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) as premium,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.rmd` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS ceded_RMD,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.partial_surrender` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS ceded_partial,
            
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.other` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS ceded_free_wd,
        IFNULL((SELECT SUM(net_withdrawal) FROM `farmers_fia.cancellation` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS ceded_cancellation,

        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.full_surrender` WHERE set_month = "{month}" AND f3.mpolicy = policy_number),0) AS full_surrenders,
        0 AS death_claim,

        IFNULL((SELECT SUM(diff * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f3.mpolicy
            AND account_number = '1-C00-4650-102'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            ) 
            GROUP by policy_id ) ,0) as market_value_adjustment,
        IFNULL((SELECT SUM(credit_amount * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f3.mpolicy
            AND account_number = '1-C00-5100-102'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            )
            GROUP by policy_id ) ,0) as surrender_charge,
        0 as dropped_policy_check,
        1 as new_policy_check
        FROM `farmers_fia.seriatim` f3
        WHERE f3.set_month = "{month}" AND f3.converge_qs >0 AND f3.mpolicy NOT IN (SELECT mpolicy FROM `farmers_fia.seriatim` WHERE set_month = "{get_previous_month(month)}")


        UNION ALL

        #Death only from death list

        SELECT 
        f4.set_month,
        f4.policy_number, 
        (SELECT DATE(missdt) FROM `farmers_fia.seriatim` WHERE mpolicy = f4.policy_number and set_month = f4.set_month) as issue_date,
        (SELECT mage FROM `farmers_fia.seriatim` WHERE mpolicy = f4.policy_number and set_month = f4.set_month) as issue_age,
        (SELECT missuest FROM `farmers_fia.seriatim` WHERE mpolicy = f4.policy_number and set_month = f4.set_month) as issue_state,
        IFNULL((SELECT mcurrbal*converge_qs FROM `farmers_fia.seriatim` WHERE mpolicy = f4.policy_number and set_month = f4.set_month)*f4.quotashare,0) as beginning_av,
        0 as end_av,
        IFNULL(f4.quotashare,0) as converge_qs,
        0 as int_in_current_month,
        0 as premium,
        0 AS ceded_RMD,
        0 AS ceded_partial,
        0 AS ceded_free_wd,
        0 AS ceded_cancellation,
        0 as full_surrender,
        IFNULL(f4.net_amount*f4.quotashare,0) as death_claim,
        0 as market_value_adjustment,
        0 as surrender_charge,
        1 as dropped_policy_check,
        0 as new_policy_check
        FROM death_list as f4 WHERE f4.quotashare >0


        UNION ALL
        #DROPPED POLICIES STARTS HERE
        #-----------------------------------------------------------------------------------------------------------------------------------# only surrender and dropped policies   
        SELECT 
        "{month}" as set_month,
        f2.mpolicy as policy_number, 
        DATE(f2.missdt) as issue_date,
        f2.mage as issue_age,
        f2.missuest as issue_state,
        IFNULL(f2.mcurrbal*f2.converge_qs,0) as beginning_av,
        0 as end_av,  
        IFNULL(f2.converge_qs,0) as converge_qs,
        IFNULL(f2.int_diff*f2.converge_qs,0) as int_in_current_month,
        IFNULL((SELECT SUM(gross_premium*quota_share) FROM `farmers_fia.premium` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) as premium,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.rmd` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS ceded_RMD,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.partial_surrender` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS ceded_partial,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.other` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS ceded_free_wd,
        IFNULL((SELECT SUM(net_withdrawal) FROM `farmers_fia.cancellation` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS ceded_cancellation,
        IFNULL((SELECT SUM(av_withdrawn*quota_share) FROM `farmers_fia.full_surrender` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS full_surrenders,
        IFNULL((SELECT SUM(net_amount) FROM `farmers_fia.death_claims` WHERE set_month = "{month}" AND f2.mpolicy = policy_number),0) AS death_claim,
            IFNULL((SELECT SUM(diff * quota_share) AS total
                FROM `farmers.combined_gl`
                WHERE set_month = "{month}"
                AND policy_id = f2.mpolicy
                AND account_number = '1-C00-4650-102'
                AND description IN (
                    'Annuity Regular Distribution',
                    'Annuity RMD',
                    'Annuity Partial Surrender',
                    'Annuity Full Surrender'
                ) 
                GROUP by policy_id ) ,0) as market_value_adjustment,

            IFNULL((SELECT SUM(credit_amount * quota_share) AS total
            FROM `farmers.combined_gl`
            WHERE set_month = "{month}"
            AND policy_id = f2.mpolicy
            AND account_number = '1-C00-5100-102'
            AND description IN (
                'Annuity Regular Distribution',
                'Annuity RMD',
                'Annuity Partial Surrender',
                'Annuity Full Surrender'
            )
            GROUP by policy_id ) ,0) as surrender_charge,
        
        1 as dropped_policy_check,
        0 as new_policy_check
        FROM `farmers_fia.seriatim` f2
        WHERE f2.current_status='Active' AND f2.set_month = "{get_previous_month(month)}" AND f2.mpolicy NOT IN (SELECT mpolicy FROM `farmers_fia.seriatim` WHERE set_month = "{month}" AND current_status='Active') and f2.mpolicy NOT IN (select policy_number FROM death_list) AND f2.mpolicy NOT IN (select policy_number from `farmers_fia.last_pending_claims` WHERE set_month = "{month}")

        ORDER BY policy_number;
        '''  
    return query

def run_avrf_analysis(set_month, product):

    print(f"AVRF analysis for {product} starting for set_month {set_month}")

    CREDS = '../../converge-database-0331482f2ee5.json'
    client = bigquery.Client.from_service_account_json(
        json_credentials_path=CREDS)
    columns = ['set_month', 'policy_number', 'issue_date', 'issue_age', 'issue_state', 
            'beginning_av', 'end_av', 'converge', 'int_in_current_month', 
            'premium', 
            'ceded_RMD', 'ceded_partial', 'ceded_free_wd', 
            'ceded_cancellation', 'full_surrenders', 'death_claim', 'market_value_adjustment', 'surrender_charge', 
            'dropped_policy_check', 'new_policy_check']
    if product == 'myga':
        query =  create_query(set_month, 'myga')
        result = client.query(query)

            # Extract the rows from the query result and convert it to a list of tuples
        data = [tuple(row) for row in result]  # Assuming each row is iterable

        avrf_result = pd.DataFrame(data, columns=columns)

        avrf_result['inflow'] = avrf_result['int_in_current_month']+avrf_result['beginning_av']-avrf_result['premium']
        avrf_result['outflow'] = avrf_result.iloc[:,10:18].sum(axis=1)
        avrf_result['exp_av'] = avrf_result['inflow']-avrf_result['outflow']
        avrf_result['diff'] = avrf_result['end_av']-avrf_result['exp_av']

        avrf_result.to_excel('Query Results/AVRF/Farmers_AVRF_'+str(set_month)+'.xlsx', index=False)
        print('exported AVRF result')
    elif product =='fia':


        farmers_fia_avrf_query= create_query(set_month, 'fia')
        result = client.query(farmers_fia_avrf_query)

        fia_avrf_result = pd.DataFrame()
            # Extract the rows from the query result and convert it to a list of tuples
        data = [tuple(row) for row in result]  # Assuming each row is iterable

    
        fia_avrf_result = pd.DataFrame(data, columns=columns)

        fia_avrf_result['inflow'] = fia_avrf_result['int_in_current_month']+fia_avrf_result['beginning_av']-fia_avrf_result['premium']
        fia_avrf_result['outflow'] = fia_avrf_result.iloc[:,10:16].sum(axis=1)
        fia_avrf_result['exp_av'] = fia_avrf_result['inflow']-fia_avrf_result['outflow']
        fia_avrf_result['diff'] = fia_avrf_result['end_av']-fia_avrf_result['exp_av']

        fia_avrf_result.to_excel('Query Results/AVRF/Farmers_FIA_AVRF_'+str(set_month)+'.xlsx', index=False)
        print('exported AVRF result')