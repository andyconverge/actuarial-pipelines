import numbers
import os
from dataclasses import dataclass, field

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
    if dataset == "kskj": qs = 0.5 
    else: qs=0.95
 
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
      cur.interest_credited                                         AS interest_credited,
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
    # outflow = all withdrawals (NOT surrender or expense charges - see
    #           note at the bottom of this file)
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

    for col in df.select_dtypes(include=['datetimetz']).columns:
        df[col] = df[col].dt.tz_localize(None)

    # ---- Summary tab + full detail in one workbook -------------------
    out_path = f'Query Results/AVRF/AVRF_{dataset}_{set_month}.xlsx'
    summary = summarize_avrf(df, set_month=set_month, dataset=dataset)
    summary.print_report()
    summary.to_excel(out_path)

    return df


# ======================================================================
# MANAGEMENT SUMMARY
# ======================================================================

# Policies are flagged for review when |diff| exceeds this dollar amount.
FLAG_THRESHOLD = 1_000.00

# Differences smaller than this are float64 noise from the quota-share
# multiplications, not real reconciling items.
ROUNDING_TOL = 0.005

STATUS_LABELS = {0: "Existing", 1: "New", 2: "Dropped"}

# Columns shown for each flagged policy.
FLAG_COLS = [
    "policy_number", "status", "plangroup",
    "beginning_fund_value", "inflow", "outflow",
    "exp_av", "end_fund_value", "diff",
]


@dataclass
class AVRFSummary:
    set_month: str
    dataset: str
    threshold: float
    headline: dict = field(default_factory=dict)
    flagged: pd.DataFrame = field(default_factory=pd.DataFrame)
    detail: pd.DataFrame = field(default_factory=pd.DataFrame)

    def print_report(self) -> None:
        h = self.headline
        w = 62
        print("=" * w)
        print(f"  AVRF SUMMARY - {self.dataset.upper()} {self.set_month}")
        print("=" * w)
        print(f"  Policies in force             {h['policies_in_force']:>16,}")
        print()
        print(f"  Beginning AV                  {h['beginning_av']:>16,.2f}")
        print(f"  + Inflow                      {h['inflow']:>16,.2f}")
        print(f"  - Outflow                     {h['outflow']:>16,.2f}")
        print(f"  = Expected end AV             {h['expected_av']:>16,.2f}")
        print(f"    Actual end AV               {h['ending_av']:>16,.2f}")
        print()
        print(f"  Total difference              {h['total_diff']:>16,.2f}")
        print(f"  Total absolute difference     {h['total_abs_diff']:>16,.2f}")
        print()
        print(f"  Policies over ${self.threshold:,.0f}: {h['flagged_policies']}")
        print("-" * w)

        if self.flagged.empty:
            print("  None - all policies within threshold.")
        else:
            show = self.flagged.copy()
            for c in FLAG_COLS[3:]:
                show[c] = show[c].map(lambda v: f"{v:,.2f}")
            print(show.to_string(index=False))
        print("=" * w)

    def to_excel(self, out_path=None, include_detail=True):
        """Write one workbook: 'Summary' laid out like print_report(), plus
        the full AVRF detail on its own tab.

        With the detail tab present the summary figures are SUM/COUNTIF
        formulas pointed at it, so every number traces back to the
        policy-level rows in the same file.
        """
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Border, Font, Side

        if out_path is None:
            out_path = f"AVRF_{self.dataset}_{self.set_month}.xlsx"
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

        h = self.headline
        money = '$#,##0.00;($#,##0.00);"-"'
        base = Font(name="Arial", size=10)
        bold = Font(name="Arial", size=10, bold=True)
        title = Font(name="Arial", size=13, bold=True)
        rule = Border(top=Side(style="thin"))

        wb = Workbook()
        ws = wb.active
        ws.title = "Summary"
        ws.sheet_view.showGridLines = False

        # Detail tab first, so the summary can reference its ranges.
        detail_ref = None
        if include_detail and not self.detail.empty:
            detail_ref = self._write_detail(wb, money, base, bold)

        def ref(col):
            """Full-column range on the detail tab for one field."""
            if detail_ref is None or col not in detail_ref:
                return ""
            letter, n_rows, sheet = detail_ref[col]
            # Sheet name contains a space, so it must stay quoted.
            return f"'{sheet}'!${letter}$2:${letter}${n_rows + 1}"

        def label(row, text, value=None, fmt=None, font=None):
            c = ws.cell(row=row, column=1, value=text)
            c.font = font or base
            if value is not None:
                v = ws.cell(row=row, column=2, value=value)
                v.font = font or base
                if fmt:
                    v.number_format = fmt
            return c

        ws["A1"] = f"AVRF SUMMARY - {self.dataset.upper()} {self.set_month}"
        ws["A1"].font = title

        live = detail_ref is not None

        label(3, "Policies in force",
              f'=COUNTIF({ref("beginning_fund_value")},">0")' if live
              else h["policies_in_force"], "#,##0", bold)

        label(5, "Beginning AV",
              f'=SUM({ref("beginning_fund_value")})' if live
              else h["beginning_av"], money)
        label(6, "+ Inflow",
              f'=SUM({ref("inflow")})' if live else h["inflow"], money)
        label(7, "- Outflow",
              f'=SUM({ref("outflow")})' if live else h["outflow"], money)
        # No leading "=" on the label: openpyxl writes any string starting
        # with "=" as a formula, which evaluates to #VALUE!.
        label(8, "Expected end AV", "=B5+B6-B7", money, bold)
        label(9, "Actual end AV",
              f'=SUM({ref("end_fund_value")})' if live else h["ending_av"],
              money, bold)
        for col in ("A", "B"):
            ws[f"{col}8"].border = rule

        # Summed from the detail rather than B9-B8: subtracting two
        # nine-figure totals loses cents to float rounding.
        label(11, "Total difference",
              f'=SUM({ref("diff")})' if live else h["total_diff"], money, bold)
        # SUMPRODUCT(ABS(...)) rather than a bare ABS array - it evaluates
        # in both Excel and LibreOffice without spill metadata.
        label(12, "Total absolute difference",
              f'=SUMPRODUCT(ABS({ref("diff")}))' if live
              else h["total_abs_diff"], money, bold)
        if not live:
            ws["C12"] = (
                "Sum of |diff| across all policies, including breaks below "
                f"the ${self.threshold:,.0f} threshold."
            )
            ws["C12"].font = Font(name="Arial", size=9, italic=True)

        # ---- flagged policies, same columns as the console report ----
        hdr_row = 15
        n = len(self.flagged)
        first, last = hdr_row + 1, hdr_row + max(n, 1)

        label(14, f"Policies over ${self.threshold:,.0f}",
              f"=COUNTA(A{first}:A{last})" if n else 0, "#,##0", bold)

        for j, name in enumerate(FLAG_COLS, start=1):
            c = ws.cell(row=hdr_row, column=j, value=name)
            c.font = bold
            c.border = Border(bottom=Side(style="thin"))
            c.alignment = Alignment(horizontal="left" if j <= 3 else "right")

        if n:
            for i, (_, r) in enumerate(self.flagged.iterrows()):
                row = first + i
                ws.cell(row=row, column=1, value=r["policy_number"]).font = base
                ws.cell(row=row, column=2, value=r["status"]).font = base
                ws.cell(row=row, column=3, value=r["plangroup"]).font = base
                for j, col in enumerate(
                    ["beginning_fund_value", "inflow", "outflow"], start=4
                ):
                    c = ws.cell(row=row, column=j, value=float(r[col]))
                    c.font, c.number_format = base, money
                # exp_av = beginning + inflow - outflow
                c = ws.cell(row=row, column=7, value=f"=D{row}+E{row}-F{row}")
                c.font, c.number_format = base, money
                c = ws.cell(row=row, column=8, value=float(r["end_fund_value"]))
                c.font, c.number_format = base, money
                # diff = actual - expected
                c = ws.cell(row=row, column=9, value=f"=H{row}-G{row}")
                c.font, c.number_format = bold, money
        else:
            ws.cell(row=first, column=1,
                    value="None - all policies within threshold.").font = base

        for col, wd in {"A": 26, "B": 18, "C": 16, "D": 20, "E": 14,
                        "F": 16, "G": 16, "H": 16, "I": 14}.items():
            ws.column_dimensions[col].width = wd

        wb.save(out_path)
        print(f"Results saved to {out_path}  ({' + '.join(wb.sheetnames)})")
        return out_path

    def _write_detail(self, wb, money, base, bold):
        """Write the full AVRF detail to its own tab.

        Returns {column_name: (column_letter, n_rows, sheet_name)} so the
        Summary tab can build SUM ranges against it.
        """
        from openpyxl.styles import Border, Side
        from openpyxl.utils import get_column_letter

        sheet = "AVRF Detail"
        ws = wb.create_sheet(sheet)
        df = self.detail

        for c in df.select_dtypes(include=["datetimetz"]).columns:
            df[c] = df[c].dt.tz_localize(None)

        cols = list(df.columns)
        for j, name in enumerate(cols, start=1):
            c = ws.cell(row=1, column=j, value=name)
            c.font = bold
            c.border = Border(bottom=Side(style="thin"))

        for row in df.itertuples(index=False, name=None):
            ws.append([v.item() if hasattr(v, "item") else v for v in row])

        # qs, the check flags and the id/date/plan columns are not currency.
        not_money = {
            "policy_number", "issue_date", "plan", "plangroup",
            "qs", "new_policy_check", "dropped_policy_check",
        }

        def is_money(name):
            """Numeric test that survives object dtype.

            A BigQuery NUMERIC column arrives as Python Decimal, so the
            frame holds object dtype and is_numeric_dtype() returns False.
            Sniff an actual value instead of trusting the dtype.
            """
            if name in not_money:
                return False
            if pd.api.types.is_numeric_dtype(df[name]):
                return True
            vals = df[name].dropna()
            return bool(len(vals)) and isinstance(vals.iloc[0], numbers.Number)

        n = len(df)
        for j, name in enumerate(cols, start=1):
            letter = get_column_letter(j)
            if is_money(name):
                for r in range(2, n + 2):
                    ws.cell(row=r, column=j).number_format = money
            if name == "issue_date":
                for r in range(2, n + 2):
                    ws.cell(row=r, column=j).number_format = "yyyy-mm-dd"
            ws.column_dimensions[letter].width = max(12, min(len(name) + 3, 30))

        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(cols))}{n + 1}"

        return {name: (get_column_letter(j), n, sheet)
                for j, name in enumerate(cols, start=1)}


def summarize_avrf(source, set_month, dataset='kskj',
                   threshold=FLAG_THRESHOLD, rounding_tol=ROUNDING_TOL):
    """Roll an AVRF detail frame up to a management-level summary.

    'Policies in force' counts rows with beginning_fund_value > 0. The
    dollar totals cover every row so the roll-forward still ties; if
    new-issue rows (beginning AV = 0) are present, a note is printed so
    the headcount is never read as the full population.
    """
    df = pd.read_excel(source) if isinstance(source, str) else source.copy()
    _validate(df)

    diff = df["diff"]
    abs_diff = diff.abs()
    is_break = abs_diff > rounding_tol
    is_flagged = abs_diff > threshold

    in_force = df["beginning_fund_value"] > 0
    n_zero_bom = int((~in_force).sum())
    if n_zero_bom:
        print(
            f"Note: {n_zero_bom} rows have beginning AV = 0 (new issues). They "
            "are excluded from the in-force count but included in the totals."
        )

    headline = {
        "policies_in_force": int(in_force.sum()),
        "beginning_av": float(df["beginning_fund_value"].sum()),
        "inflow": float(df["inflow"].sum()),
        "outflow": float(df["outflow"].sum()),
        "expected_av": float(df["exp_av"].sum()),
        "ending_av": float(df["end_fund_value"].sum()),
        "total_diff": float(diff[is_break].sum()),
        "total_abs_diff": float(abs_diff[is_break].sum()),
        "flagged_policies": int(is_flagged.sum()),
    }

    status = (
        pd.to_numeric(df["new_policy_check"], errors="coerce")
        .map(STATUS_LABELS)
        .fillna("Unknown")
    )
    flagged = (
        df.loc[is_flagged]
        .assign(status=status[is_flagged])
        .reindex(columns=FLAG_COLS)
        .sort_values("diff", key=abs, ascending=False)
        .reset_index(drop=True)
    )

    return AVRFSummary(
        set_month=set_month,
        dataset=dataset,
        threshold=threshold,
        headline=headline,
        flagged=flagged,
        detail=df,
    )


def _validate(df):
    """Fail loudly rather than silently reporting a wrong reconciliation."""
    required = {
        "policy_number", "beginning_fund_value", "end_fund_value",
        "inflow", "outflow", "exp_av", "diff", "plangroup",
        "new_policy_check",
    }
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"AVRF frame is missing required columns: {sorted(missing)}")

    if df["diff"].isna().any():
        raise ValueError(
            f"{int(df['diff'].isna().sum())} policies have a null 'diff'. Nulls "
            "understate the break total - fix the components first."
        )

    dupes = int(df["policy_number"].duplicated().sum())
    if dupes:
        print(
            f"WARNING: {dupes} duplicate policy_number rows will double-count "
            "in the totals."
        )


def check_rollforward_definition(df):
    """Test alternative outflow definitions against the actual ending AV.

    exp_av deliberately excludes surrender_charges and expense_charges.
    Tested on heartland 202607, adding them made the break worse
    ($35,477 vs $29,465), so withdrawals-only is the correct definition.
    Re-run this if a future month's break jumps unexpectedly - it
    separates a formula problem from a data problem.
    """
    base = df["beginning_fund_value"] + df["inflow"]
    withdrawals = df[[c for c in [
        "rmd_withdrawals", "free_interest_credit_withdrawals",
        "freelook_withdrawals", "cancellation_withdrawals", "Death Benefit",
        "free_partial_withdrawals", "partial_withdrawal_with_sc",
        "full_surrender_withdrawals", "internal_reissues_withdrawals",
    ] if c in df.columns]].sum(axis=1).abs()

    variants = {
        "current (withdrawals only)": withdrawals,
        "+ surrender charges": withdrawals + df["surrender_charges"].abs(),
        "+ expense charges": withdrawals + df["expense_charges"].abs(),
        "+ both charges": (withdrawals + df["surrender_charges"].abs()
                           + df["expense_charges"].abs()),
    }

    rows = []
    for name, outflow in variants.items():
        d = df["end_fund_value"] - (base - outflow)
        rows.append({
            "definition": name,
            "total_abs_diff": round(d.abs().sum(), 2),
            "net_diff": round(d.sum(), 2),
            "policies_with_break": int((d.abs() > ROUNDING_TOL).sum()),
        })
    return pd.DataFrame(rows).sort_values("total_abs_diff").reset_index(drop=True)
