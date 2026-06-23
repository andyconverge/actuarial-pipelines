import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from dateutil.relativedelta import relativedelta

CREDS = '../converge-database-0331482f2ee5.json'
client = bigquery.Client.from_service_account_json(json_credentials_path=CREDS)

QUOTA_SHARE = 0.95


def get_previous_month(set_month):
    date = datetime.strptime(set_month, "%Y%m")
    previous_month_date = date - relativedelta(months=1)
    return previous_month_date.strftime("%Y%m")


def parse_year_month(date_str):
    return datetime.strptime(date_str, "%Y%m").replace(day=1).date()


def create_query(month, dataset='kskj'):
    beginning_month = get_previous_month(month)
    qs = QUOTA_SHARE
 
    query = f'''
    -- ============================================================
    -- AVRF ANALYSIS QUERY
    -- set_month (current):    {month}
    -- beginning_month (prior): {beginning_month}
    -- quota_share (fixed):    {qs}
    -- ============================================================
 
    -- ----------------------------------------------------------------
    -- SECTION 1: EXISTING POLICIES
    -- Policies that appear in the PRIOR month's seriatim
    -- (i.e., they were already in-force at the start of this month)
    -- ----------------------------------------------------------------
    SELECT
      sv_bom.policy_number,
      sv_bom.issue_date,
 
      -- Beginning fund value = EOM of prior month (= BOM of current month)
      IFNULL(sv_bom.eom_fund_value * {qs}, 0)                          AS beginning_fund_value,
 
      -- Premium: additional premiums received THIS month
      IFNULL(sv_eom.additional_premiums_mtd * {qs}, 0)                 AS premium,
 
      -- Interest & bonus credited this month
      IFNULL(sv_eom.interest_credited * {qs}, 0)                       AS interest_credited,
      IFNULL(sv_eom.bonus_credited * {qs}, 0)                          AS bonus_credited,
 
      -- Withdrawals sourced from the withdrawals table, summed by type
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'RMD Withdrawals'
      ), 0)                                                             AS rmd_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Free Interest Withdrawal'
      ), 0)                                                             AS free_interest_credit_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Free Look Withdrawal'
      ), 0)                                                             AS freelook_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Cancellation Withdrawals'
      ), 0)                                                             AS cancellation_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Death Benefit'
      ), 0)                                                             AS death_benefit,

 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Free Partial Withdrawals'
      ), 0)                                                             AS free_partial_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Partial Withdrawal with SC'
      ), 0)                                                             AS partial_withdrawal_with_sc,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Full Surrender Withdrawals'
      ), 0)                                                             AS full_surrender_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Internal Reissue Withdrawals'
      ), 0)                                                             AS internal_reissues_withdrawals,
 
      -- Surrender charges this month from withdrawals table
      IFNULL((
        SELECT SUM(w.surrender_charge * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_bom.policy_number
          AND w.set_month = '{month}'
      ), 0)                                                             AS surrender_charges,
 
      -- Expense charges from seriatim
      IFNULL(sv_eom.expense_charges * {qs}, 0)                         AS expense_charges,
 
      -- End fund value = EOM of current month
      IFNULL(sv_eom.eom_fund_value * {qs}, 0)                          AS end_fund_value,
 
      {qs}                                                              AS qs,
 
      -- Stat reserve
      IFNULL(sv_bom.stat_reserve * {qs}, 0)                            AS beginning_reserve_stat,
      IFNULL(sv_eom.stat_reserve * {qs}, 0)                            AS end_reserve_stat,
 
      '0'                                                               AS new_policy_check,
      0                                                                 AS dropped_policy_check,
      sv_eom.plan,
      sv_eom.plangroup
 
    FROM `{dataset}.seriatim` sv_bom
 
    -- Join current month seriatim to get EOM values
    LEFT JOIN `{dataset}.seriatim` sv_eom
      ON sv_bom.policy_number = sv_eom.policy_number
     AND sv_eom.set_month = '{month}'
 
    WHERE sv_bom.set_month = '{beginning_month}' and sv_eom.eom_fund_value > 0
 
    -- ----------------------------------------------------------------
    -- SECTION 2: NEW POLICIES
    -- Policies that appear in the CURRENT month but NOT in the prior month
    -- (issued this month; beginning fund value = 0)
    -- ----------------------------------------------------------------
    UNION ALL
    SELECT
      sv_new.policy_number,
      sv_new.issue_date,
 
      0                                                                 AS beginning_fund_value,
      IFNULL((
        SELECT SUM(p.total_premium * {qs})
        FROM `{dataset}.premium` p
        WHERE p.policy_number = sv_new.policy_number
          AND p.set_month = '{month}'
      ), 0)                                                             AS premium,
 
      IFNULL(sv_new.interest_credited * {qs}, 0)                       AS interest_credited,
      IFNULL(sv_new.bonus_credited * {qs}, 0)                          AS bonus_credited,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'RMD Withdrawals'
      ), 0)                                                             AS rmd_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Free Interest Withdrawal'
      ), 0)                                                             AS free_interest_credit_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Free Look Withdrawal'
      ), 0)                                                             AS freelook_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Cancellation Withdrawals'
      ), 0)                                                             AS cancellation_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Death Benefit'
      ), 0)                                                             AS death_benefit,
                                                          
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Free Partial Withdrawals'
      ), 0)                                                             AS free_partial_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Partial Withdrawal with SC'
      ), 0)                                                             AS partial_withdrawal_with_sc,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Full Surrender Withdrawals'
      ), 0)                                                             AS full_surrender_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Internal Reissue Withdrawals'
      ), 0)                                                             AS internal_reissues_withdrawals,
 
      IFNULL((
        SELECT SUM(w.surrender_charge * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_new.policy_number
          AND w.set_month = '{month}'
      ), 0)                                                             AS surrender_charges,
 
      IFNULL(sv_new.expense_charges * {qs}, 0)                         AS expense_charges,
 
      IFNULL(sv_new.eom_fund_value * {qs}, 0)                          AS end_fund_value,
 
      {qs}                                                              AS qs,
 
      0                                                                 AS beginning_reserve_stat,
      IFNULL(sv_new.stat_reserve * {qs}, 0)                            AS end_reserve_stat,
 
      '1'                                                               AS new_policy_check,
      0                                                                 AS dropped_policy_check,
      sv_new.plan,
      sv_new.plangroup
 
    FROM `{dataset}.seriatim` sv_new
 
    WHERE sv_new.set_month = '{month}'
      -- New policy = issue date falls within this month
      AND DATE(sv_new.issue_date) >= '{parse_year_month(month)}'
      -- And does NOT exist in the prior month seriatim
      AND sv_new.policy_number NOT IN (
        SELECT policy_number
        FROM `{dataset}.seriatim`
        WHERE set_month = '{beginning_month}'
      )
 
    -- ----------------------------------------------------------------
    -- SECTION 3: DROPPED POLICIES
    -- Policies that were in the PRIOR month but are completely gone
    -- from the current month (full surrender, death, cancellation,
    -- freelook, etc.). Beginning AV = prior EOM; end AV = 0.
    -- Withdrawals are still pulled from the withdrawals table.
    -- ----------------------------------------------------------------
    UNION ALL
    SELECT
      sv_drop.policy_number,
      sv_drop.issue_date,
 
      -- Beginning fund value = EOM of prior month
      IFNULL(sv_drop.eom_fund_value * {qs}, 0)                         AS beginning_fund_value,
 
      -- No new premium for a dropped policy
      0                                                                 AS premium,
      0                                                                 AS interest_credited,
      0                                                                 AS bonus_credited,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'RMD Withdrawals'
      ), 0)                                                             AS rmd_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Free Interest Withdrawal'
      ), 0)                                                             AS free_interest_credit_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Free Look Withdrawal'
      ), 0)                                                             AS freelook_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Cancellation Withdrawals'
      ), 0)                                                             AS cancellation_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Death Benefit'
      ), 0)                                                             AS death_benefit,

 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Free Partial Withdrawals'
      ), 0)                                                             AS free_partial_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Partial Withdrawal with SC'
      ), 0)                                                             AS partial_withdrawal_with_sc,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Full Surrender Withdrawals'
      ), 0)                                                             AS full_surrender_withdrawals,
 
      IFNULL((
        SELECT SUM(w.withdrawal_amount * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
          AND w.withdrawal_type = 'Internal Reissue Withdrawals'
      ), 0)                                                             AS internal_reissues_withdrawals,
 
      IFNULL((
        SELECT SUM(w.surrender_charge * {qs})
        FROM `{dataset}.withdrawals` w
        WHERE w.policy_number = sv_drop.policy_number
          AND w.set_month = '{month}'
      ), 0)                                                             AS surrender_charges,
 
      -- No expense charges for dropped policy (already gone)
      0                                                                 AS expense_charges,
 
      -- End fund value = 0 (policy no longer exists)
      0                                                                 AS end_fund_value,
 
      {qs}                                                              AS qs,
 
      IFNULL(sv_drop.stat_reserve * {qs}, 0)                           AS beginning_reserve_stat,
      0                                                                 AS end_reserve_stat,
 
      '2'                                                               AS new_policy_check,  -- '2' = dropped
      1                                                                 AS dropped_policy_check,
      sv_drop.plan,
      sv_drop.plangroup
 
    FROM `{dataset}.seriatim` sv_drop
 
    WHERE sv_drop.set_month = '{beginning_month}' AND sv_drop.eom_fund_value>0
      -- Existed in prior month but completely absent from current month
      -- (NOT EXISTS is NULL-safe; NOT IN would return zero rows if any
      --  current-month policy_number were NULL)
      AND sv_drop.policy_number NOT IN (
        SELECT policy_number
        FROM `{dataset}.seriatim` cur
        WHERE cur.set_month = '{month}' and cur.eom_fund_value>0
      )
 
    ORDER BY policy_number
    '''
    return query


def run_avrf_analysis(set_month, dataset='kskj'):
    print(f"AVRF analysis starting for {dataset} set_month {set_month}")

    query = create_query(set_month, dataset)
    result = client.query(query)

    columns = [
        'policy_number', 'issue_date',
        'beginning_fund_value',
        'premium',
        'interest_credited', 'bonus_credited',
        'rmd_withdrawals', 'free_interest_credit_withdrawals',
        'freelook_withdrawals', 'cancellation_withdrawals',
        'Death Benefit', 
        'free_partial_withdrawals', 'partial_withdrawal_with_sc',
        'full_surrender_withdrawals', 'internal_reissues_withdrawals',
        'surrender_charges', 'expense_charges',
        'end_fund_value',
        'qs',
        'beginning_reserve_stat', 'end_reserve_stat',
        'new_policy_check',
        'dropped_policy_check',
        'plan', 'plangroup',
    ]

    data = [tuple(row) for row in result]
    df = pd.DataFrame(data, columns=columns)

    # ----------------------------------------------------------------
    # AVRF roll-forward calculation
    # inflow  = premiums + interest + bonus
    # outflow = all withdrawals + surrender charges + expense charges
    # exp_av  = beginning_fund_value + inflow - outflow
    # diff    = end_fund_value - exp_av  (should be ~0)
    # ----------------------------------------------------------------
    df['inflow'] = (
        df['premium']
        + df['interest_credited']
        + df['bonus_credited']
    ).abs()

    outflow_cols = [
        'rmd_withdrawals', 'free_interest_credit_withdrawals',
        'freelook_withdrawals', 'cancellation_withdrawals',
        'Death Benefit', 
        'free_partial_withdrawals', 'partial_withdrawal_with_sc',
        'full_surrender_withdrawals', 'internal_reissues_withdrawals'
        
    ]
    df['outflow'] = df[outflow_cols].sum(axis=1).abs()

    df['exp_av'] = df['beginning_fund_value'] + df['inflow'] - df['outflow']
    df['diff']   = df['end_fund_value'] - df['exp_av']

    sum_diff = df['diff'].abs().sum()
    print(f"Total absolute difference in AVRF AV: {sum_diff:,.2f}")

    out_path = f'Query Results/AVRF/AVRF_{dataset}_{set_month}.xlsx'
    for col in df.select_dtypes(include=['datetimetz']).columns:
      df[col] = df[col].dt.tz_localize(None)

    df.to_excel(out_path, index=False)
    print(f"Results saved to {out_path}")

    return df

