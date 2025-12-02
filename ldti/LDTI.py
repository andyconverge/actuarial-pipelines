import pandas as pd
from datetime import datetime
from google.cloud import bigquery


CREDS = '../converge-database-0331482f2ee5.json'
client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)



def silac_av_query(product):
    return f'''
        WITH total_av_split AS (
            SELECT DISTINCT 
                p.policynumber,
                totalpolicyav,
                p.plan,
                converge,
                sv.set_month
            FROM `denali.seriatim_values` sv
            JOIN `denali.policy` p 
                ON sv.policynumber = p.policynumber 
               AND p.set_month = sv.set_month
            WHERE p.set_month >= "202401"
              AND p.policynumber LIKE '{product}'
        )
        SELECT 
          set_month,
          plan,
          SUM(totalpolicyav * converge) AS net_av
        FROM total_av_split
        GROUP BY set_month, plan
        ORDER BY set_month, plan;
    '''

def myga_av_query (client):
    return f'''
    select set_month, term, count(policy_number) as ct, SUM(eom_fund_value*0.95) as net_fund_value from `{client}.seriatim`
group by set_month, term
ORDER by set_month, term;
    '''

def silac_query_adjust(policy_type):
    query =f'''
    WITH
    min_set_month AS (
      SELECT
      distinct
        policynumber,
        MIN(set_month) AS first_set_month,
        MIN(LEFT(reported_date,4)) as report_year,
      FROM `denali.policy`
      WHERE policynumber like '{policy_type}'
      GROUP BY policynumber
    ),
    inforce_list as (
      select sv.set_month, LEFT(p.reported_date,4) as report_year, count(distinct sv.policynumber) as ct_inforce from `denali.seriatim_values` sv
      JOIN `denali.policy` p ON p.policynumber = sv.policynumber AND sv.set_month = p.set_month and sv.creditstrategy = p.creditstrategy
      WHERE sv.policynumber like '{policy_type}' and totalpolicyav > 0
      GROUP by sv.set_month, LEFT(p.reported_date,4) 
      ORDER by sv.set_month, report_year
    )

    SELECT
      i.set_month,
      CAST(i.report_year as INT64) as report_year,
      i.ct_inforce,
      COUNT(m.policynumber) AS new_issued_policies
    FROM inforce_list i
    LEFT JOIN min_set_month m
      ON m.first_set_month = i.set_month AND m.report_year = i.report_year
    GROUP BY i.set_month, i.report_year, i.ct_inforce
    ORDER BY i.set_month, i.report_year;

    '''
    return query

def acl_myga_query():
    acl_myga_query ='''
           WITH min_set_month AS (
              SELECT
                sv.policy_number,
                MIN(set_month) AS first_set_month,
                LEFT(p.reported_date, 4) as report_year
              FROM `aclico.seriatim_values` sv
              JOIN `aclico.policy` p ON sv.policy_number = p.policy_number
              GROUP BY sv.policy_number, report_year
            ),
            inforce_list AS (
              SELECT
                sv.set_month,
                LEFT(p.reported_date, 4) as report_year,
                COUNT(DISTINCT sv.policy_number) AS ct_inforce
              FROM `aclico.seriatim_values` sv
              JOIN `aclico.policy` p ON sv.policy_number = p.policy_number
              WHERE sv.fund_value > 0 
              GROUP BY sv.set_month, report_year
            )
    
            SELECT
              i.set_month,
              i.report_year,
              i.ct_inforce,
              COUNT(m.policy_number) AS new_issued_policies
            FROM inforce_list i
            LEFT JOIN min_set_month m
              ON m.first_set_month = i.set_month AND m.report_year = i.report_year
            GROUP BY i.set_month, i.report_year, i.ct_inforce
            ORDER BY i.set_month, i.report_year;
    '''
    return acl_myga_query

def myga_query_adjust(client_name):
    myga_query = f'''
       WITH min_set_month AS(
          select policy_number, plangroup, MIN(set_month) as first_set_month,
          LEFT(reported_date,4) AS report_year,
          FROM `{client_name}.seriatim`
          GROUP by policy_number, plangroup,report_year
        ),
        inforce_list AS (
          select set_month, plangroup, LEFT(reported_date,4) as report_year, COUNT(distinct policy_number) as ct_inforce
          FROM `{client_name}.seriatim`
          WHERE eom_fund_value>0
          GROUP by set_month, plangroup, report_year
        )
        select i.set_month, i.plangroup, i.report_year,i.ct_inforce, count(m.policy_number) as new_issued_policies
        FROM inforce_list i
        LEFT JOIN min_set_month m
          ON m.first_set_month = i.set_month AND m.report_year = i.report_year AND i.plangroup = m.plangroup
        GROUP BY i.set_month, i.plangroup, i.report_year, i.ct_inforce
        ORDER BY i.set_month, i.plangroup, i.report_year;
    '''
    return myga_query
def farmers_query(product):
    farmers_query = '''
         WITH min_set_month AS(
          select mpolicy, MIN(set_month) as first_set_month,
          LEFT(reported_date,4) AS report_year,
          FROM `farmers.seriatim_new`
          GROUP by mpolicy, report_year
        ),
        inforce_list AS (
         select set_month, LEFT(reported_date,4) AS report_year, COUNT(distinct mpolicy) as ct_inforce
          FROM `farmers.seriatim_new`
          WHERE current_status='Active'
          GROUP by set_month,report_year
        ),
        old_min_set_month AS(
          select policy_number, MIN(set_month) as first_set_month,
          LEFT(reported_date,4) AS report_year,
          FROM `farmers.seriatim`
          GROUP by policy_number, report_year
        ),
        old_inforce_list AS (
         select set_month, LEFT(reported_date,4) AS report_year, COUNT(distinct policy_number) as ct_inforce
          FROM `farmers.seriatim`
          WHERE gross_account_value>0
          GROUP by set_month,report_year
        )
    
        select i.set_month, i.report_year,i.ct_inforce, count(m.mpolicy) as new_issued_policies
        FROM inforce_list i
        LEFT JOIN min_set_month m
          ON m.first_set_month = i.set_month AND m.report_year = i.report_year
        WHERE set_month <> '202506'
        GROUP BY i.set_month, i.report_year, i.ct_inforce
    
    
        UNION ALL
    
        select i.set_month, i.report_year,i.ct_inforce, count(m.policy_number) as new_issued_policies
        FROM old_inforce_list i
        LEFT JOIN old_min_set_month m
          ON m.first_set_month = i.set_month AND m.report_year = i.report_year
        WHERE set_month < "202506"
        GROUP BY i.set_month, i.report_year, i.ct_inforce
        UNION ALL
    
        SELECT 
      set_month,
        CASE 
          WHEN reported_date = set_month THEN LEFT(set_month, 4)   -- new policies use set_month year
          ELSE LEFT(reported_date, 4)                              -- inforce use reported_date year
        END AS report_year,
        SUM(CASE WHEN current_status='Active' THEN 1 ELSE 0 END) AS ct_inforce,
        SUM(CASE WHEN reported_date = set_month THEN 1 ELSE 0 END) AS new_issued_policies,
        
      FROM `farmers.seriatim_new`
      WHERE set_month = '202506'
        AND current_status = 'Active'
      GROUP BY set_month, report_year
      ORDER BY set_month, report_year;
    '''

    farmers_fia_query = '''
     WITH min_set_month AS (
              SELECT
                sv.policy_id,
                MIN(set_month) AS first_set_month,
                LEFT(sv.reported_date, 4) as report_year
              FROM `farmers_fia.rsv` sv
              GROUP BY sv.policy_id, report_year
            ),
            inforce_list AS (
              SELECT
                sv.set_month,
                LEFT(sv.reported_date, 4) as report_year,
                COUNT(DISTINCT sv.policy_id) AS ct_inforce
              FROM `farmers_fia.rsv` sv
              WHERE sv.gross_account_value > 0 
              GROUP BY sv.set_month, report_year
            )
    
            SELECT
              i.set_month,
              i.report_year,
              i.ct_inforce,
              COUNT(m.policy_id) AS new_issued_policies
            FROM inforce_list i
            LEFT JOIN min_set_month m
              ON m.first_set_month = i.set_month AND m.report_year = i.report_year
            GROUP BY i.set_month, i.report_year, i.ct_inforce
            ORDER BY i.set_month, i.report_year;
    '''
    return farmers_query if product =='myga' else farmers_fia_query

def life_query_run():
    
    life_premium = '''
        WITH premium_tranche_new as (
        select policyno, gross_premium, set_month, (SELECT MIN(pmdissuedt) from lifetemp.seriatim WHERE policyno = pmdno) as issuedate from lifetemp.premiums 
        )
        SELECT set_month, count(*) as ct, SUM(gross_premium)*0.95 as net_prem,
        CASE WHEN DATE(issuedate) < DATE("2017-09-30") THEN 1
          WHEN DATE(issuedate) BETWEEN DATE("2017-10-01") AND DATE("2018-09-30")  THEN 2
          WHEN DATE(issuedate) BETWEEN DATE("2018-10-01") AND DATE("2020-09-30")  THEN 3
          WHEN DATE(issuedate) BETWEEN DATE("2020-10-01") AND DATE("2022-09-30")  THEN 4
          END tranche_mapping
         from premium_tranche_new
         GROUP by set_month, tranche_mapping
         ORDER by set_month, tranche_mapping;
     '''
     
     # Run the query
    life_premium_result = client.query(life_premium)
    result_iter = life_premium_result.result()
    # Dynamically fetch column names from the first row
    column_names = [field.name for field in result_iter.schema]

    # Convert rows to DataFrame
    rows = [list(row) for row in result_iter]
    df_premium = pd.DataFrame(rows, columns=column_names)


    life_query = '''
    WITH life_tranche AS (
      SELECT 
        sv.pmdno, 
        sv.set_month,
        sv.pbfamt, 
        sv.pmdissuedt AS issue_date, 
        LPAD(sv.pbfrsvcd1, 5, '0') AS pbfrsvcd1,
        CASE 
          WHEN DATE(sv.pmdissuedt) < DATE("2017-09-30") THEN 1
          WHEN DATE(sv.pmdissuedt) BETWEEN DATE("2017-10-01") AND DATE("2018-09-30") THEN 2
          WHEN DATE(sv.pmdissuedt) BETWEEN DATE("2018-10-01") AND DATE("2020-09-30") THEN 3
          WHEN DATE(sv.pmdissuedt) BETWEEN DATE("2020-10-01") AND DATE("2022-09-30") THEN 4
        END AS tranche_mapping,
        v.exhibit,
        CASE 
          WHEN v.exhibit = '5A' THEN sv.pbfamt
          WHEN v.exhibit = '5B' THEN a.total2
        END AS faceamt,
        a.total2
      FROM lifetemp.seriatim sv
      LEFT JOIN lifetemp.annuity a 
        ON a.policyno = sv.pmdno AND a.set_month = sv.set_month
      LEFT JOIN lifetemp.valcode v 
        ON LPAD(sv.pbfrsvcd1, 5, '0') = v.valcode_new
      WHERE v.exhibit IN ('5A', '5B')
    ),
    count_pol AS (
      SELECT 
        s.set_month,
        CASE 
          WHEN DATE(s.pmdissuedt) < DATE("2017-09-30") THEN 1
          WHEN DATE(s.pmdissuedt) BETWEEN DATE("2017-10-01") AND DATE("2018-09-30") THEN 2
          WHEN DATE(s.pmdissuedt) BETWEEN DATE("2018-10-01") AND DATE("2020-09-30") THEN 3
          WHEN DATE(s.pmdissuedt) BETWEEN DATE("2020-10-01") AND DATE("2022-09-30") THEN 4
        END AS tranche_mapping, 
        COUNT(DISTINCT s.pmdno) AS ct
      FROM lifetemp.seriatim s
      LEFT JOIN lifetemp.valcode v 
        ON LPAD(s.pbfrsvcd1, 5, '0') = v.valcode_new
      WHERE v.exhibit IN ('5A', '5B') 
      GROUP BY s.set_month, tranche_mapping
    )
    SELECT 
      l.set_month, 
      l.tranche_mapping, 
      c.ct AS count, 
      SUM(l.faceamt) * 0.95 AS famt
    FROM life_tranche AS l
    LEFT JOIN count_pol c 
      ON c.set_month = l.set_month 
      AND c.tranche_mapping = l.tranche_mapping
    WHERE l.exhibit IN ('5A', '5B')
    GROUP BY l.set_month, l.tranche_mapping, c.ct
    ORDER BY l.set_month, l.tranche_mapping;
    '''


    # Run the life query
    life_query_result = client.query(life_query)
    result_life = life_query_result.result()

    # Dynamically fetch column names from the schema
    column_names = [field.name for field in result_life.schema]

    # Convert rows to DataFrame
    rows = [list(row) for row in result_life]
    df_premium_life = pd.DataFrame(rows, columns=column_names)

    life_claim = '''
    WITH claims_tranche AS (
      SELECT 
        policyno, 
        net_payable, 
        set_month,
        CASE 
          WHEN DATE(issue_date) < DATE("2017-09-30") THEN 1
          WHEN DATE(issue_date) BETWEEN DATE("2017-10-01") AND DATE("2018-09-30") THEN 2
          WHEN DATE(issue_date) BETWEEN DATE("2018-10-01") AND DATE("2020-09-30") THEN 3
          WHEN DATE(issue_date) BETWEEN DATE("2020-10-01") AND DATE("2022-09-30") THEN 4
        END AS tranche_mapping, 
      FROM lifetemp.claims
    )
    SELECT 
      set_month, 
      COUNT(policyno) AS ct,
      tranche_mapping, 
      SUM(net_payable) * 0.95 AS claim_amount
    FROM claims_tranche
    GROUP BY set_month, tranche_mapping
    ORDER BY set_month, tranche_mapping;
    '''

    # Run the claim query
    life_claim_result = client.query(life_claim)
    result_claim = life_claim_result.result()

    # Dynamically fetch column names
    claim_column_names = [field.name for field in result_claim.schema]

    # Convert rows to DataFrame
    claim_rows = [list(row) for row in result_claim]
    df_life_claim = pd.DataFrame(claim_rows, columns=claim_column_names)


    df_merged = pd.merge(df_premium_life, df_premium, 
                         on=["set_month", "tranche_mapping"], how="left")

    df_merged = pd.merge(df_merged, df_life_claim, 
                         on=["set_month", "tranche_mapping"], how="left")

    df_merged = df_merged.rename(columns = {"ct_x" : "prem_ct", "ct_y" : "claim_ct"})
    
    df_merged.to_excel("life_ldti.xlsx", sheet_name='Data', index=False)

def export_to_result (excel_name, df, av_df=None):
    # Create an Excel writer object
    with pd.ExcelWriter(f"{excel_name}.xlsx") as writer:
        #Writing data
        df.to_excel(writer, sheet_name='Data', index=False)
        # Write the first pivot table to the first sheet
        pivot_df = df.pivot_table(
            index='report_year',
            columns='set_month',
            values='new_issued_policies',
            aggfunc ='sum',
            fill_value=0
        )
        #nb new business
        pivot_df.to_excel(writer, sheet_name='NB', index=True)

        # Write the second pivot table to inforce sheet
        inforce_pivot_df = df.pivot_table(
            index='report_year',
            columns='set_month',
            values='ct_inforce',
            aggfunc ='sum',
            fill_value=0
        )
        inforce_pivot_df.to_excel(writer, sheet_name='IF', index=True)
        
        # ➕ NEW — Add AV calculation sheet if provided
        
        if av_df is not None:
            
            av_df.to_excel(writer, sheet_name='AV', index=False)

    print("Data exported successfully :", excel_name)
    
def run_query_and_export(query, columns, export_name):
    """Runs a query, converts results to a DataFrame, and exports it."""
    result = client.query(query)
    rows = [list(row) for row in result.result()]
    print(rows)
    df = pd.DataFrame(rows, columns=columns)
    export_to_result(export_name, df)
    return df

def run_av_query_and_export(query):
    result = client.query(query)
    rows = [list(row) for row in result.result()]
    df = pd.DataFrame(rows, columns=["set_month", "term", "ct", "net_fund_value"])
    return df


def av_query_adjust(client_name):
    # Query for kskj and heartland
    av_query = f"""
        SELECT 
            set_month, 
            term, 
            COUNT(policy_number) AS ct, 
            SUM(eom_fund_value * 0.95) AS net_fund_value
        FROM `{client_name}.seriatim`
        GROUP BY set_month, term
        ORDER BY set_month, term;
    """

    # ACL MyGA query
    acl_myga = """
        SELECT 
            sv.set_month, 
            t.term, 
            COUNT(sv.policy_number) AS ct,
            SUM(sv.fund_value * 
                (CASE 
                    WHEN sv.reins_flag ='P' THEN 0.65 
                    ELSE 0.40 
                 END)) AS net_fund_value
        FROM `aclico.seriatim_values` sv
        JOIN `aclico.term_table` t ON sv.plan = t.plan
        WHERE sv.set_month >= "202401"
        GROUP BY sv.set_month, t.term
        ORDER BY sv.set_month, t.term;
    """

    # Correct conditional return
    if client_name in ["kskj", "heartland"]:
        return av_query
    elif client_name == "acl_myga":
        return acl_myga
    else:
        raise ValueError(f"Unknown client_name: {client_name}")


def main_query_run(client):
    print('Starting LDTI run for :', client)
    heartland_column = ['set_month','plangroup','report_year', 'ct_inforce', 'new_issued_policies']
    new_pol_columns = ['set_month','report_year', 'ct_inforce', 'new_issued_policies']
    if client =='SILAC':
        for i in ['D%', 'T%']:
            fia_query = silac_query_adjust(i)
            print(fia_query)
            run_query_and_export(
                fia_query,
                new_pol_columns,
                f"silac ldti result_{i}"
            )
        
    elif client == 'ACL MYGA':
       query = acl_myga_query()
       print('test')
       print(query)
       df_main= run_query_and_export(
           query,
           new_pol_columns,
           "acl_myga ldti result"
           )
       av_query = av_query_adjust('acl_myga')
       av_df = run_av_query_and_export(av_query)
   
       # Re-export everything in one file including AV
       export_to_result("ACL MYGA ldti result", df_main, av_df=av_df)
       print("MYGA LDTI/AV Finished")
    elif client =='Heartland':
       print('starting Heartland LDTI')
       query = myga_query_adjust('heartland')
       print(query)
       df_main = run_query_and_export(
           query,
           heartland_column,
           "heartland ldti result"
           )
       print("Heartland LDTI Finished")
       av_query = av_query_adjust('heartland')
       av_df = run_av_query_and_export(av_query)
   
       # Re-export everything in one file including AV
       export_to_result("Heartland ldti result", df_main, av_df=av_df)
       
    elif client =='KSKJ':
        print('starting KSKJ LDTI')
        # Main MYGA query
        query = myga_query_adjust('kskj')
        df_main = run_query_and_export(
            query,
            heartland_column,
            "KSKJ ldti result"  # Will save to KSKJ ldti result.xlsx
        )
    
        # AV query
        av_query = av_query_adjust('kskj')
        av_df = run_av_query_and_export(av_query)
    
        # Re-export everything in one file including AV
        export_to_result("KSKJ ldti result", df_main, av_df=av_df)
    
        print("KSKJ LDTI Finished with AV")
    elif client =="ACL Life":
        print('starting ACL Life LDTI')
        query = life_query_run()
        print(query)
        life_query_run()
        print("ACL Life LDTI Finished")
    elif client =='Farmers MYGA':
        print('starting Farmers MYGA')
        query = farmers_query('myga')
        run_query_and_export(
            query,
            new_pol_columns,
            "Farmers MYGA ldti result"
            )
        print('Farmers MYGA done')
    elif client =='Farmers FIA':
        print('starting Farmers FIA')
        query = farmers_query('FIA')
        run_query_and_export(
            query,
            new_pol_columns,
            "Farmers FIA ldti result"
            )
        print('Farmers FIA done')
        
    
#This part is not done. I need to add Account Value calculation on a separate sheet. 
#ADD ACCOUNT VALUE RUN AS WELL and add rest of the clients. If case statement. 
