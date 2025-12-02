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


def create_query(month):

    beginning_month = get_previous_month(month)
    query = f'''
    CREATE TEMP FUNCTION
      quota_share(reins_flag STRING) AS (
        CASE
          WHEN reins_flag ="P" THEN 0.65
          ELSE 0.4
      END
        );
    SELECT
      p.policy_number AS policy_number,
      p.date_issued AS date_issued,
      IFNULL(sv.fund_value*quota_share(sv.reins_flag),0) AS beginning_fund_value,
      IFNULL((
        SELECT
          purchase_price*quota_share(reins_flag)
        FROM
          `aclico.seriatim_values`
        WHERE
          set_month = "{month}"
          AND p.policy_number = policy_number) - (sv.purchase_price*quota_share(sv.reins_flag)),0) AS premium,
      IFNULL((
        SELECT
          interest_earned*quota_share(reins_flag)
        FROM
          `aclico.seriatim_values`
        WHERE
          set_month = "{month}"
          AND p.policy_number = policy_number) - sv.interest_earned*quota_share(sv.reins_flag),0) AS interest_earned,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv.reins_flag))
        FROM
          `aclico.full_surrenders`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS full_surrender,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv.reins_flag))
        FROM
          `aclico.cancellations`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS cancellation,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv.reins_flag))
        FROM
          `aclico.surrender_fees`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS surrender_fees,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv.reins_flag))
        FROM
          `aclico.aiw`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS aiw,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv.reins_flag))
        FROM
          `aclico.rmd`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS rmd,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv.reins_flag))
        FROM
          `aclico.penalty_free`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS penalty_free,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv.reins_flag))
        FROM
          `aclico.partial_surrenders`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS partial_surrender,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv.reins_flag))
        FROM
          `aclico.death_claims`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS death_claims,
      IFNULL((
        SELECT
          fund_value*quota_share(sv.reins_flag)
        FROM
          `aclico.seriatim_values`
        WHERE
          set_month ="{month}"
          AND p.policy_number = policy_number),0) AS end_fund_value,
      quota_share(sv.reins_flag) AS qs,
      IFNULL(sv.stat_reserve*quota_share(sv.reins_flag),0) AS beginning_reserve_stat,
      IFNULL((
        SELECT
          stat_reserve*quota_share(sv.reins_flag)
        FROM
          `aclico.seriatim_values`
        WHERE
          set_month = "{month}"
          AND policy_number = p.policy_number),0) AS end_reserve_stat,
      "0" AS new_policy_check,
      (
      SELECT
        val_code
      FROM
        `aclico.seriatim_values`
      WHERE
        set_month = "{month}"
        AND policy_number = p.policy_number) AS val_code,
    FROM
      `aclico.seriatim_values` sv
    JOIN
      `aclico.policy` p
    ON
      sv.policy_number = p.policy_number
    WHERE
      set_month = "{beginning_month}"
      
      #NEW POLICIES starts here #-----------------------------------------------#
    UNION ALL
    SELECT
      p.policy_number AS policy_number,
      p.date_issued AS date_issued,
      0 AS beginning_fund_value,
      IFNULL(sv1.purchase_price*quota_share(sv1.reins_flag),0) AS premium,
      IFNULL(sv1.interest_earned*quota_share(sv1.reins_flag),0) AS interest_earned,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv1.reins_flag))
        FROM
          `aclico.full_surrenders`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS full_surrender,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv1.reins_flag))
        FROM
          `aclico.cancellations`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS cancellation,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv1.reins_flag))
        FROM
          `aclico.surrender_fees`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS surrender_fees,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv1.reins_flag))
        FROM
          `aclico.aiw`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS aiw,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv1.reins_flag))
        FROM
          `aclico.rmd`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS rmd,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv1.reins_flag))
        FROM
          `aclico.penalty_free`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS penalty_free,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv1.reins_flag))
        FROM
          `aclico.partial_surrenders`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS partial_surrender,
      IFNULL((
        SELECT
          SUM(withdrawal_amount*quota_share(sv1.reins_flag))
        FROM
          `aclico.death_claims`
        WHERE
          policy_number = p.policy_number
          AND set_month ="{month}"
        GROUP BY
          policy_number),0) AS death_claims,
      IFNULL((
        SELECT
          fund_value*quota_share(sv1.reins_flag)
        FROM
          `aclico.seriatim_values`
        WHERE
          set_month ="{month}"
          AND p.policy_number = policy_number),0) AS end_fund_value,
      quota_share(sv1.reins_flag) AS qs,
      0 AS beginnning_reserve_stat,
      IFNULL(sv1.stat_reserve*quota_share(sv1.reins_flag),0) AS end_reserve_stat,
      "1" AS new_policy_check,
      sv1.val_code AS val_code,
    FROM
      `aclico.seriatim_values` sv1
    JOIN
      `aclico.policy` p
    ON
      sv1.policy_number = p.policy_number
    WHERE
      set_month = "{month}"
      AND p.date_issued >= '{parse_year_month(month)}'
    ORDER BY
      policy_number
    '''
    return query


def run_avrf_analysis(set_month):

    print("AVRF analysis for MYGA starting for set_month", set_month)

    CREDS = '../../converge-database-0331482f2ee5.json'
    client = bigquery.Client.from_service_account_json(
        json_credentials_path=CREDS)
    query = create_query(set_month)
    result = client.query(query)

    avrf_result = pd.DataFrame()

    # Define the column headers based on the provided mapping
    columns = ['policy_number', 'date_issued', 'beginning_fund_value', 'premium', 'interest_earned',
               'full_surrender', 'cancellation', 'surrender_fees', 'aiw',
               'rmd', 'penalty_free', 'partial_surrender', 'death_claims',
               'end_fund_value', 'qs', 'beginning_reserve_stat', 'end_reserve_stat', 'new_policy_check', 'val_code']

    # Extract the rows from the query result and convert it to a list of tuples
    data = [tuple(row) for row in result]  # Assuming each row is iterable

    # Create a DataFrame from the data and column headers

    avrf_result = pd.DataFrame(data, columns=columns)

    avrf_result['inflow'] = abs(
        avrf_result['premium']+avrf_result['interest_earned'])
    avrf_result['outflow'] = abs(avrf_result['full_surrender']+avrf_result['cancellation']+avrf_result['aiw'] +
                                 avrf_result['rmd']+avrf_result['penalty_free']+avrf_result['partial_surrender']+avrf_result['death_claims'])
    avrf_result['exp_av'] = avrf_result['beginning_fund_value'] + \
        avrf_result['inflow']-avrf_result['outflow']
    avrf_result['diff'] = avrf_result['end_fund_value']-avrf_result['exp_av']

    sum_diff = avrf_result['diff'].abs().sum()
    
    print('difference in avrf av: ',sum_diff)

    avrf_result.to_excel('Query Results/AVRF/AVRF_'+str(set_month)+'.xlsx', index=False)
