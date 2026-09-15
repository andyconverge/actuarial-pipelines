"""ACLICO / MYGA AVRF analysis.

    df = run_avrf_analysis('202607')

A policy is FLAGGED when its AV roll-forward does not tie:

    inflow  = premium + interest_earned
    outflow = full_surrender + cancellation + aiw + rmd + penalty_free
              + partial_surrender + death_claims
    exp_av  = beginning_fund_value + inflow - outflow
    diff    = end_fund_value - exp_av        flagged when |diff| > 0.005

A flagged policy then splits two ways, and only one of them is work:

    is_recaptured = TRUE   explained. The policy left the ceded block because
                           the cession changed, not because the fund paid out.
                           No claim exists and none should.
    is_recaptured = FALSE  NEEDS REVIEW. Look at it by hand.

surrender_fees is excluded from outflow - it is a charge, not a fund movement.
Confirmed for 202607: withdrawals-only ties to 0.00, while including surrender
fees breaks 32 policies by 23,316.43.

RECAPTURE
=========
Sourced from `aclico.recaptures`, joined on policy_number, restricted to
renew_date on or before the month end being reported. recaptured_av on that
table is already at the Converge share (gross AV at renewal x quota share), so
it needs no further scaling here.

Recapture is NOT treated as an outflow and does NOT enter exp_av. The fund did
not pay out - the cession changed - so folding it into the roll-forward would
misstate what the policyholder's money did. It is a label on the difference,
not a component of it. Whether the treaty should account for it differently is
a question for the team, and changing that is a change to exp_av, not to the
join.

If your column names differ from the ones below, the `recapture` CTE is the
only place to edit:
    policy_number, renew_date, recaptured_av

WHAT EACH FLAGGED ROW CARRIES
=============================
  is_recaptured   TRUE when `aclico.recaptures` has a row for this policy
                  effective on or before month end.
  recapture_date  the renewal date the recapture took effect.
  recaptured_av   Converge share of the AV recaptured.
  left_extract    1 = no seriatim row this month; the policy is gone.
  txn_count       transaction rows across all eight claims tables. 0 = no
                  claim recorded anywhere.
  status          Existing / New / Run-off in extract / Left extract.
                  "Run-off in extract" = still listed but AV went to zero,
                  i.e. a full surrender that stays on the seriatim.
  val_code, reins_flag, qs   carried forward from the prior month for
                  departures, so the row is identifiable rather than NULLs.

WHAT 202607 LOOKED LIKE, for comparison next month
==================================================
  Total difference                  -11,702,495.68
  Policies flagged                             306
    of which recaptured                        306
    NEEDS REVIEW                                 0

  Every policy present in both months reconciled to 0.00. The entire
  difference was departures, and all 306 are covered by the recapture table
  (194 from the Jan-Jun Policy List backlog, 112 from the July settlement tab,
  less the ones whose June AV was already zero).

FIXES CARRIED IN THIS VERSION
=============================
1. Three explicit query sections instead of one that let departures fall out
   through IFNULL: in both months / new this month / left the extract.
2. txn_count and left_extract returned per policy.
3. .abs() removed from inflow and outflow. It flipped the sign of a reversal -
   when a withdrawal is refunded the cumulative delta goes negative, and abs()
   turned money coming back IN into money going OUT, double-counting the
   error. That was 4 policies and $7,091.81 of the 202607 break.
4. quota_share bound consistently to the outer row's reins_flag. Previously
   the premium and interest subqueries used an unqualified quota_share(
   reins_flag) that bound to the inner table, so a policy whose flag moved
   between "P" and anything else had a 0.40 figure subtracted from a 0.65 one.
5. Correlated subqueries replaced by CTEs.
6. Recapture joined from `aclico.recaptures`.

NOTES ON THE DATA
=================
  * purchase_price and interest_earned are cumulative inception-to-date, so
    premium and interest are current-minus-prior deltas. They can legitimately
    be negative when a transaction is reversed - never wrap them in abs().
  * quota share is 0.65 for reins_flag "P" and 0.40 for V / 3D / AF / 3C.
    Verified against Ceded Stat Res / Stat Reserve with zero variance. Do NOT
    use the Reins Pct column (0.9 / 0.4) - that is ACL's own cession, not the
    Converge share.
  * A policy that is recaptured but still present in the extract is possible -
    the settlement workbook shows cession dropping 0.9 -> 0.25 rather than to
    zero on some policies. summarize_avrf() counts these and warns; they are
    worth querying with ACL rather than ignoring.
"""

import numbers
import os
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery

CREDS = '../converge-database-0331482f2ee5.json'

# Differences below this are float64 noise from the quota-share
# multiplications, not reconciling items.
ROUNDING_TOL = 0.005

# (bigquery table, output column) for every withdrawal source.
TXN_SOURCES = [
    ('full_surrenders',    'full_surrender'),
    ('cancellations',      'cancellation'),
    ('surrender_fees',     'surrender_fees'),
    ('aiw',                'aiw'),
    ('rmd',                'rmd'),
    ('penalty_free',       'penalty_free'),
    ('partial_surrenders', 'partial_surrender'),
    ('death_claims',       'death_claims'),
]

OUTFLOW_COLS = [
    'full_surrender', 'cancellation', 'aiw', 'rmd', 'penalty_free',
    'partial_surrender', 'death_claims',
]

COLUMNS = [
    'policy_number', 'date_issued', 'beginning_fund_value', 'premium',
    'interest_earned', 'full_surrender', 'cancellation', 'surrender_fees',
    'aiw', 'rmd', 'penalty_free', 'partial_surrender', 'death_claims',
    'end_fund_value', 'qs', 'beginning_reserve_stat', 'end_reserve_stat',
    'new_policy_check', 'val_code', 'reins_flag', 'left_extract', 'txn_count',
    'is_recaptured', 'recapture_date', 'recaptured_av',
]

STATUS_LABELS = {0: "Existing", 1: "New", 2: "Left extract"}

# Columns shown on the flagged tabs.
FLAG_COLS = [
    "policy_number", "status", "needs_review", "is_recaptured",
    "recapture_date", "recaptured_av",
    "val_code", "reins_flag", "qs", "left_extract", "txn_count", "date_issued",
    "beginning_fund_value", "inflow", "outflow",
    "exp_av", "end_fund_value", "diff", "beginning_reserve_stat",
]


def get_client(creds=CREDS):
    return bigquery.Client.from_service_account_json(json_credentials_path=creds)


def get_previous_month(set_month):
    date = datetime.strptime(set_month, "%Y%m")
    return (date - relativedelta(months=1)).strftime("%Y%m")


def parse_year_month(date_str):
    return datetime.strptime(date_str, "%Y%m").replace(day=1).date()


# ======================================================================
# QUERY
# ======================================================================
def create_query(month, dataset='aclico'):
    beginning_month = get_previous_month(month)

    txn_union = "\n      UNION ALL ".join(
        f'''SELECT policy_number, '{col}' AS bucket, withdrawal_amount AS amt
                 FROM `{dataset}.{tbl}` WHERE set_month = "{month}"'''
        for tbl, col in TXN_SOURCES
    )
    txn_pivot = ",\n        ".join(
        f"SUM(IF(bucket = '{col}', amt, 0)) AS {col}" for _, col in TXN_SOURCES
    )
    txn_cols = ",\n      ".join(
        f"IFNULL(t.{col} * quota_share(s.reins_flag), 0) AS {col}"
        for _, col in TXN_SOURCES
    )
    # Identical in all three sections.
    rec_cols = """r.policy_number IS NOT NULL                                AS is_recaptured,
      r.recapture_date                                          AS recapture_date,
      IFNULL(r.recaptured_av, 0)                                AS recaptured_av"""

    return f'''
    -- ============================================================
    -- AVRF ANALYSIS - ACLICO MYGA
    -- set_month (current):     {month}
    -- beginning_month (prior): {beginning_month}
    -- quota share: 0.65 for reins_flag "P", 0.40 otherwise
    --   (validated against Ceded Stat Res / Stat Reserve, zero variance)
    -- ============================================================
    CREATE TEMP FUNCTION quota_share(reins_flag STRING) AS (
      CASE WHEN reins_flag = "P" THEN 0.65 ELSE 0.4 END
    );

    WITH
    bom AS (
      SELECT * FROM `{dataset}.seriatim_values` WHERE set_month = "{beginning_month}"
    ),
    eom AS (
      SELECT * FROM `{dataset}.seriatim_values` WHERE set_month = "{month}"
    ),

    -- All eight withdrawal sources, scanned once each and pivoted.
    -- txn_count says whether ANY claim activity was recorded this month.
    txn_raw AS (
      {txn_union}
    ),
    txn AS (
      SELECT
        policy_number,
        {txn_pivot},
        COUNT(*) AS txn_count
      FROM txn_raw
      GROUP BY policy_number
    ),

    -- Recaptures effective on or before this month end. Grouped because a
    -- policy can be recaptured more than once - the settlement workbook shows
    -- cession dropping 0.9 -> 0.25 rather than to zero on some policies, so a
    -- further recapture later is possible. MIN() takes the first effective
    -- date; SUM() totals the AV released to date.
    --
    -- recaptured_av is already at the Converge share - do NOT multiply by
    -- quota_share() again.
    recapture AS (
      SELECT
        policy_number,
        MIN(renew_date)    AS recapture_date,
        SUM(recaptured_av) AS recaptured_av
      FROM `{dataset}.recaptures`
      WHERE renew_date <= LAST_DAY(PARSE_DATE('%Y%m', "{month}"))
      GROUP BY policy_number
    )

    -- ----------------------------------------------------------------
    -- SECTION 1: in BOTH months.
    -- purchase_price and interest_earned are cumulative, so premium and
    -- interest are deltas. They can be negative when a transaction is
    -- reversed - do not abs() them downstream.
    -- ----------------------------------------------------------------
    SELECT
      p.policy_number,
      p.date_issued,

      IFNULL(s.fund_value * quota_share(s.reins_flag), 0)                AS beginning_fund_value,
      IFNULL((e.purchase_price  - s.purchase_price)
             * quota_share(s.reins_flag), 0)                             AS premium,
      IFNULL((e.interest_earned - s.interest_earned)
             * quota_share(s.reins_flag), 0)                             AS interest_earned,

      {txn_cols},

      IFNULL(e.fund_value * quota_share(s.reins_flag), 0)                AS end_fund_value,
      quota_share(s.reins_flag)                                          AS qs,
      IFNULL(s.stat_reserve * quota_share(s.reins_flag), 0)              AS beginning_reserve_stat,
      IFNULL(e.stat_reserve * quota_share(s.reins_flag), 0)              AS end_reserve_stat,
      "0"                                                                AS new_policy_check,
      e.val_code                                                         AS val_code,
      s.reins_flag                                                       AS reins_flag,
      0                                                                  AS left_extract,
      IFNULL(t.txn_count, 0)                                             AS txn_count,
      {rec_cols}

    FROM bom s
    JOIN `{dataset}.policy` p ON p.policy_number = s.policy_number
    JOIN eom e                ON e.policy_number = s.policy_number
    LEFT JOIN txn t           ON t.policy_number = s.policy_number
    LEFT JOIN recapture r     ON r.policy_number = s.policy_number

    -- ----------------------------------------------------------------
    -- SECTION 2: NEW ISSUES. Cumulative equals the month's activity.
    -- ----------------------------------------------------------------
    UNION ALL
    SELECT
      p.policy_number,
      p.date_issued,

      0                                                                  AS beginning_fund_value,
      IFNULL(s.purchase_price  * quota_share(s.reins_flag), 0)           AS premium,
      IFNULL(s.interest_earned * quota_share(s.reins_flag), 0)           AS interest_earned,

      {txn_cols},

      IFNULL(s.fund_value * quota_share(s.reins_flag), 0)                AS end_fund_value,
      quota_share(s.reins_flag)                                          AS qs,
      0                                                                  AS beginning_reserve_stat,
      IFNULL(s.stat_reserve * quota_share(s.reins_flag), 0)              AS end_reserve_stat,
      "1"                                                                AS new_policy_check,
      s.val_code                                                         AS val_code,
      s.reins_flag                                                       AS reins_flag,
      0                                                                  AS left_extract,
      IFNULL(t.txn_count, 0)                                             AS txn_count,
      {rec_cols}

    FROM eom s
    JOIN `{dataset}.policy` p ON p.policy_number = s.policy_number
    LEFT JOIN txn t           ON t.policy_number = s.policy_number
    LEFT JOIN recapture r     ON r.policy_number = s.policy_number
    WHERE p.date_issued >= '{parse_year_month(month)}'
      AND NOT EXISTS (SELECT 1 FROM bom b WHERE b.policy_number = s.policy_number)

    -- ----------------------------------------------------------------
    -- SECTION 3: LEFT THE EXTRACT - in the prior month, gone this month.
    --
    -- These used to disappear into IFNULL(...) = 0. There is no current-month
    -- row, so no final interest is knowable and no ending AV exists: the
    -- beginning AV runs off to zero. val_code and reins_flag come from the
    -- PRIOR month - the only copy left. This is where recaptures land.
    -- ----------------------------------------------------------------
    UNION ALL
    SELECT
      p.policy_number,
      p.date_issued,

      IFNULL(s.fund_value * quota_share(s.reins_flag), 0)                AS beginning_fund_value,
      0                                                                  AS premium,
      0                                                                  AS interest_earned,

      {txn_cols},

      0                                                                  AS end_fund_value,
      quota_share(s.reins_flag)                                          AS qs,
      IFNULL(s.stat_reserve * quota_share(s.reins_flag), 0)              AS beginning_reserve_stat,
      0                                                                  AS end_reserve_stat,
      "2"                                                                AS new_policy_check,
      s.val_code                                                         AS val_code,
      s.reins_flag                                                       AS reins_flag,
      1                                                                  AS left_extract,
      IFNULL(t.txn_count, 0)                                             AS txn_count,
      {rec_cols}

    FROM bom s
    JOIN `{dataset}.policy` p ON p.policy_number = s.policy_number
    LEFT JOIN txn t           ON t.policy_number = s.policy_number
    LEFT JOIN recapture r     ON r.policy_number = s.policy_number
    WHERE NOT EXISTS (SELECT 1 FROM eom e WHERE e.policy_number = s.policy_number)

    ORDER BY policy_number
    '''


def run_avrf_analysis(set_month, dataset='aclico', creds=CREDS, out_path=None):
    print(f"AVRF analysis for MYGA starting for set_month {set_month}")

    client = get_client(creds)
    result = client.query(create_query(set_month, dataset))
    df = pd.DataFrame([tuple(r) for r in result], columns=COLUMNS)

    df = add_rollforward(df)

    for col in df.select_dtypes(include=['datetimetz']).columns:
        df[col] = df[col].dt.tz_localize(None)

    if out_path is None:
        out_path = f'Query Results/AVRF/AVRF_{set_month}.xlsx'

    summary = summarize_avrf(df, set_month=set_month)
    summary.print_report()
    summary.to_excel(out_path)
    return df


def add_rollforward(df):
    """Attach inflow / outflow / exp_av / diff / status / flagged / needs_review.

    No .abs() anywhere - a negative inflow or outflow is a reversal and must
    stay negative.

    Recapture deliberately does not enter exp_av. It labels the difference; it
    is not a component of it.
    """
    df = df.copy()
    df['inflow'] = df['premium'] + df['interest_earned']
    df['outflow'] = df[OUTFLOW_COLS].sum(axis=1)
    df['exp_av'] = df['beginning_fund_value'] + df['inflow'] - df['outflow']
    df['diff'] = df['end_fund_value'] - df['exp_av']
    df['status'] = _derive_status(df)
    df['flagged'] = df['diff'].abs() > ROUNDING_TOL

    recaptured = df.get('is_recaptured', False)
    if not isinstance(recaptured, pd.Series):
        recaptured = pd.Series(False, index=df.index)
    df['is_recaptured'] = recaptured.fillna(False).astype(bool)

    # The only column anyone has to act on.
    df['needs_review'] = df['flagged'] & ~df['is_recaptured']
    return df


def _derive_status(df):
    """Existing / New / Left extract from the query flag, plus Run-off.

    "Run-off in extract" = still listed this month but AV went to zero, i.e.
    a full surrender that stays on the seriatim. Distinct from having left.
    """
    status = (
        pd.to_numeric(df["new_policy_check"], errors="coerce")
        .map(STATUS_LABELS)
        .fillna("Unknown")
    )
    if "left_extract" in df.columns:
        runoff = (
            (pd.to_numeric(df["left_extract"], errors="coerce") == 0)
            & (df["beginning_fund_value"] > 0)
            & (df["end_fund_value"] <= 0)
        )
        status = status.mask(runoff, "Run-off in extract")
    return status


# ======================================================================
# SUMMARY
# ======================================================================
@dataclass
class AVRFSummary:
    set_month: str
    headline: dict = field(default_factory=dict)
    needs_review: pd.DataFrame = field(default_factory=pd.DataFrame)
    flagged: pd.DataFrame = field(default_factory=pd.DataFrame)
    detail: pd.DataFrame = field(default_factory=pd.DataFrame)

    def print_report(self) -> None:
        h = self.headline
        w = 68
        print("=" * w)
        print(f"  AVRF SUMMARY - ACLICO MYGA {self.set_month}")
        print("=" * w)
        print(f"  Policies in force             {h['policies_in_force']:>18,}")
        print(f"  New issues                    {h['new_policies']:>18,}")
        print(f"  Run-off in extract            {h['runoff_policies']:>18,}")
        print(f"  Left the extract              {h['left_extract']:>18,}")
        print(f"  Recaptured                    {h['recaptured_policies']:>18,}")
        print()
        print(f"  Beginning AV                  {h['beginning_av']:>18,.2f}")
        print(f"  + Inflow                      {h['inflow']:>18,.2f}")
        print(f"  - Outflow                     {h['outflow']:>18,.2f}")
        print(f"  = Expected end AV             {h['expected_av']:>18,.2f}")
        print(f"    Actual end AV               {h['ending_av']:>18,.2f}")
        print()
        print(f"  TOTAL DIFFERENCE              {h['total_diff']:>18,.2f}")
        print(f"  Policies flagged              {h['flagged_policies']:>18,}")
        print(f"    of which recaptured         {h['flagged_recaptured']:>18,}"
              f"   {h['recaptured_diff']:>16,.2f}")
        print("-" * w)
        print(f"  NEEDS REVIEW                  {h['needs_review']:>18,}"
              f"   {h['needs_review_diff']:>16,.2f}")
        print("-" * w)
        if self.needs_review.empty:
            print("  Nothing to review - every flagged policy is a recapture.")
        else:
            show = self.needs_review.head(15).copy()
            cols = ["policy_number", "status", "val_code", "reins_flag",
                    "left_extract", "txn_count", "beginning_fund_value", "diff"]
            cols = [c for c in cols if c in show.columns]
            for c in ("beginning_fund_value", "diff"):
                if c in show.columns:
                    show[c] = show[c].map(lambda v: f"{v:,.2f}")
            print(show[cols].to_string(index=False))
            if len(self.needs_review) > 15:
                print(f"  ... and {len(self.needs_review) - 15} more on the "
                      "'Needs review' tab")
        if h['recaptured_still_ceded']:
            print("-" * w)
            print(f"  WARNING: {h['recaptured_still_ceded']} policies are "
                  "recaptured but still in the extract.")
            print("  Partial recapture is possible (cession 0.9 -> 0.25), but")
            print("  confirm with ACL rather than assuming.")
        print("=" * w)

    def to_excel(self, out_path=None, include_detail=True):
        from openpyxl import Workbook
        from openpyxl.styles import Border, Font, Side

        if out_path is None:
            out_path = f"AVRF_{self.set_month}.xlsx"
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

        detail_ref = None
        if include_detail and not self.detail.empty:
            detail_ref = self._write_detail(wb, money, base, bold)

        def ref(col):
            if detail_ref is None or col not in detail_ref:
                return ""
            letter, n_rows, sheet = detail_ref[col]
            return f"'{sheet}'!${letter}$2:${letter}${n_rows + 1}"

        def label(row, text, value=None, fmt=None, font=None, note=None):
            c = ws.cell(row=row, column=1, value=text)
            c.font = font or base
            if value is not None:
                v = ws.cell(row=row, column=2, value=value)
                v.font = font or base
                if fmt:
                    v.number_format = fmt
            if note:
                nc = ws.cell(row=row, column=3, value=note)
                nc.font = Font(name="Arial", size=9, italic=True)

        ws["A1"] = f"AVRF SUMMARY - ACLICO MYGA {self.set_month}"
        ws["A1"].font = title

        live = detail_ref is not None
        label(3, "Policies in force",
              f'=COUNTIF({ref("beginning_fund_value")},">0")' if live
              else h["policies_in_force"], "#,##0", bold)
        label(4, "New issues", h["new_policies"], "#,##0")
        label(5, "Run-off in extract", h["runoff_policies"], "#,##0")
        label(6, "Left the extract", h["left_extract"], "#,##0")
        label(7, "Recaptured", h["recaptured_policies"], "#,##0")

        label(9, "Beginning AV",
              f'=SUM({ref("beginning_fund_value")})' if live
              else h["beginning_av"], money)
        label(10, "+ Inflow",
              f'=SUM({ref("inflow")})' if live else h["inflow"], money)
        label(11, "- Outflow",
              f'=SUM({ref("outflow")})' if live else h["outflow"], money)
        # No leading "=" on a label: openpyxl writes any string starting with
        # "=" as a formula, which evaluates to #VALUE!.
        label(12, "Expected end AV", "=B9+B10-B11", money, bold)
        label(13, "Actual end AV",
              f'=SUM({ref("end_fund_value")})' if live else h["ending_av"],
              money, bold)
        for col in ("A", "B"):
            ws[f"{col}12"].border = rule

        # Summed from the detail rather than B13-B12: subtracting two
        # nine-figure totals loses cents to float rounding.
        label(15, "TOTAL DIFFERENCE",
              f'=SUM({ref("diff")})' if live else h["total_diff"], money, bold)
        label(16, "Policies flagged", h["flagged_policies"], "#,##0")
        label(17, "  of which recaptured", h["flagged_recaptured"], "#,##0",
              base, note="Explained - cession changed, no claim should exist.")
        label(18, "  their difference", h["recaptured_diff"], money)
        label(20, "NEEDS REVIEW", h["needs_review"], "#,##0", bold,
              note="See the 'Needs review' tab. These are the only ones to work.")
        label(21, "  their difference", h["needs_review_diff"], money, bold)

        if h["recaptured_still_ceded"]:
            label(23, "WARNING: recaptured but still in extract",
                  h["recaptured_still_ceded"], "#,##0", bold,
                  note="Partial recapture is possible - confirm with ACL.")

        for col, wd in {"A": 38, "B": 20, "C": 52}.items():
            ws.column_dimensions[col].width = wd

        self._write_frame(wb, "Needs review", self.needs_review, money, base, bold)
        self._write_frame(wb, "Flagged - all", self.flagged, money, base, bold)

        wb.save(out_path)
        print(f"Results saved to {out_path}  ({' + '.join(wb.sheetnames)})")
        return out_path

    def _write_frame(self, wb, sheet, df, money, base, bold):
        from openpyxl.styles import Border, Side
        from openpyxl.utils import get_column_letter
        ws = wb.create_sheet(sheet[:31])
        if df is None or df.empty:
            ws.cell(row=1, column=1, value="None.").font = base
            return
        for j, name in enumerate(df.columns, start=1):
            c = ws.cell(row=1, column=j, value=str(name))
            c.font = bold
            c.border = Border(bottom=Side(style="thin"))
            ws.column_dimensions[get_column_letter(j)].width = max(
                12, min(len(str(name)) + 3, 30))
        for row in df.itertuples(index=False, name=None):
            ws.append([v.item() if hasattr(v, "item") else v for v in row])
        for j, name in enumerate(df.columns, start=1):
            if name in ("beginning_fund_value", "inflow", "outflow", "exp_av",
                        "end_fund_value", "diff", "beginning_reserve_stat",
                        "recaptured_av"):
                for rr in range(2, len(df) + 2):
                    ws.cell(row=rr, column=j).number_format = money
            if name in ("date_issued", "recapture_date"):
                for rr in range(2, len(df) + 2):
                    ws.cell(row=rr, column=j).number_format = "yyyy-mm-dd"
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(df.columns))}{len(df) + 1}"

    def _write_detail(self, wb, money, base, bold):
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

        not_money = {
            "policy_number", "date_issued", "val_code", "reins_flag", "qs",
            "new_policy_check", "status", "left_extract", "txn_count",
            "flagged", "is_recaptured", "recapture_date", "needs_review",
        }

        def is_money(name):
            """Numeric test that survives object dtype.

            A BigQuery NUMERIC column arrives as Python Decimal, so the frame
            holds object dtype and is_numeric_dtype() returns False. Sniff an
            actual value instead of trusting the dtype.
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
            if name in ("date_issued", "recapture_date"):
                for r in range(2, n + 2):
                    ws.cell(row=r, column=j).number_format = "yyyy-mm-dd"
            ws.column_dimensions[letter].width = max(12, min(len(name) + 3, 30))

        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(cols))}{n + 1}"
        return {name: (get_column_letter(j), n, sheet)
                for j, name in enumerate(cols, start=1)}


def summarize_avrf(source, set_month, rounding_tol=ROUNDING_TOL):
    """Roll an AVRF detail frame up to a summary plus the review list."""
    df = pd.read_excel(source) if isinstance(source, str) else source.copy()
    _validate(df)

    if "needs_review" not in df.columns:
        df = add_rollforward(df)

    diff = df["diff"]
    is_break = diff.abs() > rounding_tol
    in_force = df["beginning_fund_value"] > 0
    rec = df["is_recaptured"]
    review = df["needs_review"]
    left = pd.to_numeric(df.get("left_extract", 0), errors="coerce").fillna(0) == 1

    n_zero_bom = int((~in_force).sum())
    if n_zero_bom:
        print(
            f"Note: {n_zero_bom} rows have beginning AV = 0. They are excluded "
            "from the in-force count but included in the totals."
        )

    headline = {
        "policies_in_force": int(in_force.sum()),
        "new_policies": int((df["status"] == "New").sum()),
        "runoff_policies": int((df["status"] == "Run-off in extract").sum()),
        "left_extract": int((df["status"] == "Left extract").sum()),
        "recaptured_policies": int(rec.sum()),
        "beginning_av": float(df["beginning_fund_value"].sum()),
        "inflow": float(df["inflow"].sum()),
        "outflow": float(df["outflow"].sum()),
        "expected_av": float(df["exp_av"].sum()),
        "ending_av": float(df["end_fund_value"].sum()),
        "total_diff": float(diff[is_break].sum()),
        "flagged_policies": int(is_break.sum()),
        "flagged_recaptured": int((is_break & rec).sum()),
        "recaptured_diff": float(diff[is_break & rec].sum()),
        "needs_review": int(review.sum()),
        "needs_review_diff": float(diff[review].sum()),
        # Recaptured yet still being ceded - possible with a partial
        # recapture, but worth querying rather than assuming.
        "recaptured_still_ceded": int((rec & ~left).sum()),
    }

    cols = [c for c in FLAG_COLS if c in df.columns]
    flagged = (df.loc[is_break].reindex(columns=cols)
               .sort_values(["needs_review", "diff"],
                            key=lambda s: s.abs() if s.name == "diff" else s,
                            ascending=[False, False])
               .reset_index(drop=True))
    needs_review = (df.loc[review].reindex(columns=cols)
                    .sort_values("diff", key=abs, ascending=False)
                    .reset_index(drop=True))

    return AVRFSummary(
        set_month=set_month,
        headline=headline,
        needs_review=needs_review,
        flagged=flagged,
        detail=df,
    )


def _validate(df):
    """Fail loudly rather than silently reporting a wrong reconciliation."""
    required = {
        "policy_number", "beginning_fund_value", "end_fund_value",
        "premium", "interest_earned", "new_policy_check",
    }
    missing = required - set(df.columns)
    if missing:
        raise KeyError(f"AVRF frame is missing required columns: {sorted(missing)}")

    if "is_recaptured" not in df.columns:
        print(
            "WARNING: no is_recaptured column - the recapture join did not run. "
            "Every departure will show as NEEDS REVIEW."
        )

    if "diff" in df.columns and df["diff"].isna().any():
        raise ValueError(
            f"{int(df['diff'].isna().sum())} policies have a null 'diff'. Nulls "
            "understate the break total - fix the components first."
        )

    dupes = int(df["policy_number"].duplicated().sum())
    if dupes:
        print(
            f"WARNING: {dupes} duplicate policy_number rows will double-count "
            "in the totals. Check `aclico.policy`, `seriatim_values`, and that "
            "`aclico.recaptures` has at most one row per policy per renew_date."
        )


# ======================================================================
# OPTIONAL CHECKS - call these when a month looks wrong
# ======================================================================
def tie_to_settlement(seriatim_path, expected_ceded_reserve):
    """Confirm the extract's ceded stat reserve ties to the settlement sheet.

    202607 tied to the cent both months. If this breaks, stop - the extract
    and the settlement disagree and nothing downstream is trustworthy.
    """
    d = pd.read_excel(seriatim_path, 'Reserves by policy')
    d = d.loc[:, ~d.columns.astype(str).str.startswith('Unnamed')]
    actual = float(d['Ceded to Converge Stat Res'].sum())
    delta = actual - expected_ceded_reserve
    print(f"  extract ceded stat reserve  {actual:>18,.2f}")
    print(f"  settlement sheet            {expected_ceded_reserve:>18,.2f}")
    print(f"  difference                  {delta:>18,.2f}"
          f"{'   OK' if abs(delta) < 0.5 else '   *** DOES NOT TIE ***'}")
    return delta


def check_quota_share(seriatim_path):
    """Verify quota_share against Ceded Stat Res / Stat Reserve per reins flag.

    Expect exactly 0.65 for "P" and exactly 0.40 for everything else, with
    zero spread. Any flag showing a range means the hardcoded CASE is stale.
    """
    d = pd.read_excel(seriatim_path, 'Reserves by policy')
    d = d.loc[:, ~d.columns.astype(str).str.startswith('Unnamed')]
    d = d[d['Stat Reserve'].abs() > 0].copy()
    d['implied'] = d['Ceded to Converge Stat Res'] / d['Stat Reserve']
    out = (d.groupby('Reins Flag')['implied']
           .agg(['count', 'min', 'max'])
           .assign(expected=lambda x: np.where(x.index == 'P', 0.65, 0.40))
           .round(6))
    out['OK'] = (out['min'] - out['expected']).abs().lt(1e-9) & \
                (out['max'] - out['expected']).abs().lt(1e-9)
    return out


def check_seriatim_identity(seriatim_path):
    """Fund Value must equal Purchase Price + Interest Earned - Withdrawals.

    Held for all 15,996 rows in 202607 at 2e-10. This is what makes the
    cumulative-delta approach to premium and interest valid; if it fails, the
    deltas are meaningless.
    """
    d = pd.read_excel(seriatim_path, 'Reserves by policy')
    d = d.loc[:, ~d.columns.astype(str).str.startswith('Unnamed')]
    resid = (d['Purchase Price'] + d['Interest Earned']
             - d['Withdrawals'] - d['Fund Value'])
    print(f"  rows: {len(d):,}   max |residual|: {resid.abs().max():.10f}"
          f"   rows off by >0.005: {int((resid.abs() > 0.005).sum())}")
    return resid


def check_rollforward_definition(df):
    """Test whether surrender_fees belongs in outflow.

    202607: withdrawals-only ties to 0.00, including surrender fees breaks 32
    policies by 23,316.43. Restricted to policies present in both months, or
    the departures swamp the comparison.
    """
    d = df[pd.to_numeric(df.get("left_extract", 0), errors="coerce").fillna(0) == 0]
    base = d["beginning_fund_value"] + d["inflow"]
    withdrawals = d[[c for c in OUTFLOW_COLS if c in d.columns]].sum(axis=1)

    variants = {
        "current (withdrawals only)": withdrawals,
        "+ surrender fees": withdrawals + d["surrender_fees"],
    }
    rows = []
    for name, outflow in variants.items():
        dd = d["end_fund_value"] - (base - outflow)
        rows.append({
            "definition": name,
            "total_abs_diff": round(dd.abs().sum(), 2),
            "net_diff": round(dd.sum(), 2),
            "policies_with_break": int((dd.abs() > ROUNDING_TOL).sum()),
        })
    return pd.DataFrame(rows).sort_values("total_abs_diff").reset_index(drop=True)


def check_recapture_coverage(df):
    """Departures with no claim and no recapture row - the real review list.

    Run after a load of `aclico.recaptures` to confirm the table covers the
    month. On 202607 this returns empty: all 306 departures are recaptures.
    """
    left = pd.to_numeric(df.get("left_extract", 0), errors="coerce").fillna(0) == 1
    txn = pd.to_numeric(df.get("txn_count", 0), errors="coerce").fillna(0)
    gap = left & (txn == 0) & ~df["is_recaptured"] & (df["beginning_fund_value"] > 0)
    out = df.loc[gap, ["policy_number", "val_code", "reins_flag", "qs",
                       "beginning_fund_value", "diff"]]
    print(f"  {len(out)} departures with no claim and no recapture row"
          f"   {out['beginning_fund_value'].sum():,.2f} of ceded AV")
    return out.sort_values("beginning_fund_value", ascending=False)


def status_breakdown(df):
    """Where the difference sits by status - run this first when a month looks off."""
    d = df if "status" in df.columns else add_rollforward(df)
    return (
        d.groupby("status")
        .agg(policies=("policy_number", "count"),
             beginning_av=("beginning_fund_value", "sum"),
             inflow=("inflow", "sum"),
             outflow=("outflow", "sum"),
             ending_av=("end_fund_value", "sum"),
             net_diff=("diff", "sum"),
             abs_diff=("diff", lambda s: s.abs().sum()))
        .round(2)
        .sort_values("abs_diff", ascending=False)
    )
