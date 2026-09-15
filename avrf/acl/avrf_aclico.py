"""ACLICO / MYGA AVRF analysis with management summary and break classification.

WHAT CHANGED IN THIS VERSION
============================
The 202607 run showed a $11,695,403.87 break that decomposed completely:

    A. Recaptured                                112 policies   -4,765,287.39
    B. Left extract - no claim activity          194 policies   -6,937,208.29
    C. In extract - reversal (abs() bug)           4 policies       +7,091.81
                                                                ---------------
                                                                -11,695,403.87

None of it was a data error. It was AV leaving the ceded block through routes
the roll-forward had no column for. So the roll-forward now has columns for
them, and the summary reports a bridge instead of one undifferentiated break.

1. THE QUERY IS NOW THREE EXPLICIT SECTIONS (as in the Heartland version)
   rather than one section that let departures fall out through IFNULL:
     1 - in both months
     2 - new issues this month
     3 - in the prior month, ABSENT from the current extract
   Section 3 carries val_code and reins_flag forward from the prior month, so
   departures arrive identifiable instead of as rows of NULLs. The population
   is unchanged, so totals are comparable to the previous version.

2. txn_count IS RETURNED PER POLICY. This is the column that answers "why did
   it drop". A policy with left_extract = 1 and txn_count = 0 left the block
   with no claim recorded anywhere - the exception list that needs ACL
   follow-up. No cross-reference to the settlement workbook required.

3. .abs() IS GONE from inflow and outflow. It silently flipped the sign of a
   reversal: when a withdrawal is refunded the cumulative-withdrawal delta
   goes negative, and abs() turned money coming back IN into money going OUT,
   a double-counted error of 2x the reversal. That was the whole of category C.
   Only 4 of 16,308 policies had a reversal in 202607, so this is rare but it
   is pure error when it happens.

4. THE QUOTA SHARE IS NOW BOUND CONSISTENTLY. Previously the premium and
   interest subqueries used an unqualified quota_share(reins_flag), which
   binds to the INNER table's flag, while the subtracted term used the outer
   row's. A policy whose flag moved between "P" and anything else was having a
   0.40 figure subtracted from a 0.65 one. (Verified zero flag changes among
   202607 survivors, so this did not bite that month - but it is a live trap.)

5. CORRELATED SUBQUERIES REPLACED BY CTEs. Eight transaction tables scanned
   once each and pivoted, instead of one correlated subquery per column per
   row. Cheaper, and it makes the quota-share application visible in one place.

VALIDATION ANCHORS (202607) - re-run these each month
=====================================================
  * Ceded stat reserve in the seriatim must tie to the settlement sheet:
      June 355,647,773.25 = settlement BOM;  July 337,859,119.08 = EOM. Both
      tied to the cent.
  * The identity Fund Value = Purchase Price + Interest Earned - Withdrawals
      held for all 15,996 July rows (max residual 2e-10). Withdrawals is
      cumulative inception-to-date, which is why premium and interest are
      computed as current-minus-prior deltas.
  * quota_share is correct: Ceded Stat Res / Stat Reserve is exactly 0.65 for
      reins_flag "P" and exactly 0.40 for V / 3D / AF / 3C, zero variance.
      Do NOT use the Reins Pct column (0.9 / 0.4) - that is ACL's own
      cession, not the Converge share.

STILL OPEN - do not assert these to the team as settled
=======================================================
  * Category B is labelled "no claim activity", not "non-renewal". The
    settlement sheet's "Decrease from Non-Renewals (gross of SC)" line is
    -5,136,540.59 ceded, while those policies carry 7,224,880.56 of June
    ceded stat reserve. The renewal dates (all on or before month end, issue
    years clustered at 2016 and 2019) point at maturities, but the amounts do
    not tie. Ask ACL to confirm the disposition of the policies on the
    exception list.
  * Recapture is treated as its own bridge line, not as an outflow, because
    the fund did not pay out - the cession changed. Whether it belongs inside
    the AV roll-forward at all is a treaty question for the team.
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

# (bigquery table, output column) for every withdrawal source.
# Add a non-renewal table here if ACL exposes one - that alone would move
# category B out of the break.
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

# surrender_fees is deliberately excluded - it is a charge, not a fund
# movement. check_rollforward_definition() re-tests that each month.
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
    # Every money column below is multiplied by the policy's quota share.
    txn_cols = ",\n      ".join(
        f"IFNULL(t.{col} * quota_share(s.reins_flag), 0) AS {col}"
        for _, col in TXN_SOURCES
    )
    txn_zero = ",\n      ".join(f"0 AS {col}" for _, col in TXN_SOURCES)

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
    -- Prior and current month seriatim, isolated once each.
    bom AS (
      SELECT * FROM `{dataset}.seriatim_values` WHERE set_month = "{beginning_month}"
    ),
    eom AS (
      SELECT * FROM `{dataset}.seriatim_values` WHERE set_month = "{month}"
    ),

    -- All eight withdrawal sources, scanned once each and pivoted.
    -- txn_count is the whole point of this CTE: it says whether ANY claim
    -- activity was recorded for the policy this month. A departure with
    -- txn_count = 0 left the ceded block with no claim behind it.
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
    )

    -- ----------------------------------------------------------------
    -- SECTION 1: policies present in BOTH months.
    -- purchase_price and interest_earned are cumulative inception-to-date,
    -- so premium and interest are current-minus-prior deltas. The deltas can
    -- legitimately be NEGATIVE when a transaction is reversed - do not wrap
    -- them in ABS anywhere downstream.
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
      IFNULL(t.txn_count, 0)                                             AS txn_count

    FROM bom s
    JOIN `{dataset}.policy` p ON p.policy_number = s.policy_number
    JOIN eom e                ON e.policy_number = s.policy_number
    LEFT JOIN txn t           ON t.policy_number = s.policy_number

    -- ----------------------------------------------------------------
    -- SECTION 2: NEW ISSUES - in the current month only.
    -- Cumulative equals the month's activity, so no delta is taken.
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
      IFNULL(t.txn_count, 0)                                             AS txn_count

    FROM eom s
    JOIN `{dataset}.policy` p ON p.policy_number = s.policy_number
    LEFT JOIN txn t           ON t.policy_number = s.policy_number
    WHERE p.date_issued >= '{parse_year_month(month)}'
      AND NOT EXISTS (SELECT 1 FROM bom b WHERE b.policy_number = s.policy_number)

    -- ----------------------------------------------------------------
    -- SECTION 3: LEFT THE EXTRACT - in the prior month, gone this month.
    --
    -- These are the rows that used to disappear into IFNULL(...) = 0. There
    -- is no current-month row, so no final-month interest is knowable and no
    -- ending AV exists: beginning AV runs off to zero. That run-off is real,
    -- but it is NOT a reconciling error, so it must be identified rather than
    -- left inside diff.
    --
    -- val_code and reins_flag come from the PRIOR month - they are the only
    -- copy left, and without them the exception list is a page of NULLs.
    --
    -- 202607: 306 such policies, 11,708,135.71 of ceded AV. 112 were treaty
    -- recaptures (reins code P/V/AF/3C -> PR/VR/AFR/3A, and the extract
    -- carries no "-R" codes at all, so they vanish). The other 194 had
    -- txn_count = 0 - gone with no claim recorded.
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
      IFNULL(t.txn_count, 0)                                             AS txn_count

    FROM bom s
    JOIN `{dataset}.policy` p ON p.policy_number = s.policy_number
    LEFT JOIN txn t           ON t.policy_number = s.policy_number
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
    """Attach inflow / outflow / exp_av / diff / status / break_category.

    No .abs() anywhere. A negative inflow or outflow is a reversal and must
    stay negative; abs() would turn a refund into a payment and double the
    error. See the module docstring, item 3.
    """
    df = df.copy()
    df['inflow'] = df['premium'] + df['interest_earned']
    df['outflow'] = df[OUTFLOW_COLS].sum(axis=1)
    df['exp_av'] = df['beginning_fund_value'] + df['inflow'] - df['outflow']
    df['diff'] = df['end_fund_value'] - df['exp_av']
    df['status'] = _derive_status(df)
    df['break_category'] = _derive_break_category(df)
    return df


# ======================================================================
# CLASSIFICATION
# ======================================================================
FLAG_THRESHOLD = 1_000.00
ROUNDING_TOL = 0.005

STATUS_LABELS = {0: "Existing", 1: "New", 2: "Left extract"}

# The bridge categories. Everything except UNEXPLAINED is a known route out of
# the ceded block; UNEXPLAINED is the only one that is a reconciliation
# problem, and it is the number to quote as "the break".
CAT_NONE = "0. No break"
CAT_LEFT_NO_CLAIM = "A. Left extract - no claim recorded"
CAT_LEFT_WITH_CLAIM = "B. Left extract - claim recorded, AV not covered"
CAT_REVERSAL = "C. Reversal in the month"
CAT_UNEXPLAINED = "D. Unexplained - investigate"

FLAG_COLS = [
    "policy_number", "status", "break_category", "val_code", "reins_flag",
    "txn_count", "beginning_fund_value", "inflow", "outflow",
    "exp_av", "end_fund_value", "diff",
]

EXCEPTION_COLS = [
    "policy_number", "val_code", "reins_flag", "date_issued", "qs",
    "txn_count", "beginning_fund_value", "beginning_reserve_stat", "diff",
]


def _derive_status(df):
    status = (
        pd.to_numeric(df["new_policy_check"], errors="coerce")
        .map(STATUS_LABELS)
        .fillna("Unknown")
    )
    # Still listed in the extract but run off to zero AV - a full surrender
    # that remains on the seriatim. Distinct from having left the extract.
    if "left_extract" in df.columns:
        runoff = (
            (pd.to_numeric(df["left_extract"], errors="coerce") == 0)
            & (df["beginning_fund_value"] > 0)
            & (df["end_fund_value"] <= 0)
        )
        status = status.mask(runoff, "Run-off in extract")
    return status


def _derive_break_category(df):
    left = pd.to_numeric(df.get("left_extract", 0), errors="coerce").fillna(0) == 1
    txn = pd.to_numeric(df.get("txn_count", 0), errors="coerce").fillna(0)
    is_break = df["diff"].abs() > ROUNDING_TOL

    reversal = (df["inflow"] < -ROUNDING_TOL) | (df["outflow"] < -ROUNDING_TOL)

    cat = pd.Series(CAT_UNEXPLAINED, index=df.index, dtype=object)
    cat = cat.mask(left & (txn == 0), CAT_LEFT_NO_CLAIM)
    cat = cat.mask(left & (txn > 0), CAT_LEFT_WITH_CLAIM)
    cat = cat.mask(~left & reversal, CAT_REVERSAL)
    return cat.mask(~is_break, CAT_NONE)


@dataclass
class AVRFSummary:
    set_month: str
    threshold: float
    headline: dict = field(default_factory=dict)
    bridge: pd.DataFrame = field(default_factory=pd.DataFrame)
    flagged: pd.DataFrame = field(default_factory=pd.DataFrame)
    exceptions: pd.DataFrame = field(default_factory=pd.DataFrame)
    detail: pd.DataFrame = field(default_factory=pd.DataFrame)

    def print_report(self) -> None:
        h = self.headline
        w = 76
        print("=" * w)
        print(f"  AVRF SUMMARY - ACLICO MYGA {self.set_month}")
        print("=" * w)
        print(f"  Policies in force             {h['policies_in_force']:>18,}")
        print(f"  New issues                    {h['new_policies']:>18,}")
        print(f"  Run-off in extract            {h['runoff_policies']:>18,}")
        print(f"  Left the extract              {h['left_extract']:>18,}")
        print()
        print(f"  Beginning AV                  {h['beginning_av']:>18,.2f}")
        print(f"  + Inflow                      {h['inflow']:>18,.2f}")
        print(f"  - Outflow                     {h['outflow']:>18,.2f}")
        print(f"  = Expected end AV             {h['expected_av']:>18,.2f}")
        print(f"    Actual end AV               {h['ending_av']:>18,.2f}")
        print()
        print(f"  Total difference              {h['total_diff']:>18,.2f}")
        print("-" * w)
        print("  BRIDGE - how the difference is accounted for")
        print("-" * w)
        if self.bridge.empty:
            print("  No differences above tolerance.")
        else:
            b = self.bridge.copy()
            b["diff"] = b["diff"].map(lambda v: f"{v:,.2f}")
            b["beginning_av"] = b["beginning_av"].map(lambda v: f"{v:,.2f}")
            print(b.to_string())
        print("-" * w)
        print(f"  TRUE UNEXPLAINED BREAK        {h['unexplained_diff']:>18,.2f}"
              f"   ({h['unexplained_policies']} policies)")
        print("=" * w)
        if not self.exceptions.empty:
            print(f"  {len(self.exceptions)} policies left the extract with NO claim "
                  f"recorded, carrying {h['left_no_claim_av']:,.2f} of ceded AV.")
            print("  These need a disposition from ACL. Top 10 by AV:")
            top = self.exceptions.nlargest(10, "beginning_fund_value").copy()
            for c in ("beginning_fund_value", "beginning_reserve_stat", "diff"):
                top[c] = top[c].map(lambda v: f"{v:,.2f}")
            print(top.to_string(index=False))
            print("=" * w)

    def to_excel(self, out_path=None, include_detail=True):
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Border, Font, Side

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

        label(8, "Beginning AV",
              f'=SUM({ref("beginning_fund_value")})' if live
              else h["beginning_av"], money)
        label(9, "+ Inflow",
              f'=SUM({ref("inflow")})' if live else h["inflow"], money)
        label(10, "- Outflow",
              f'=SUM({ref("outflow")})' if live else h["outflow"], money)
        # No leading "=" on a label: openpyxl writes any string starting with
        # "=" as a formula, which evaluates to #VALUE!.
        label(11, "Expected end AV", "=B8+B9-B10", money, bold)
        label(12, "Actual end AV",
              f'=SUM({ref("end_fund_value")})' if live else h["ending_av"],
              money, bold)
        for col in ("A", "B"):
            ws[f"{col}11"].border = rule

        # Summed from the detail rather than B12-B11: subtracting two
        # nine-figure totals loses cents to float rounding.
        label(14, "Total difference",
              f'=SUM({ref("diff")})' if live else h["total_diff"], money, bold)

        r = 16
        ws.cell(row=r, column=1, value="BRIDGE").font = bold
        r += 1
        hdr = ["category", "policies", "beginning_av", "diff"]
        for j, name in enumerate(hdr, start=1):
            c = ws.cell(row=r, column=j, value=name)
            c.font = bold
            c.border = Border(bottom=Side(style="thin"))
        r += 1
        for cat, row in self.bridge.iterrows():
            ws.cell(row=r, column=1, value=str(cat)).font = base
            ws.cell(row=r, column=2, value=int(row["policies"])).font = base
            for j, k in ((3, "beginning_av"), (4, "diff")):
                c = ws.cell(row=r, column=j, value=float(row[k]))
                c.font, c.number_format = base, money
            r += 1

        r += 1
        label(r, "TRUE UNEXPLAINED BREAK", h["unexplained_diff"], money, bold,
              note=f"{h['unexplained_policies']} policies. This is the figure "
                   "to quote as the reconciliation break.")

        for col, wd in {"A": 40, "B": 18, "C": 20, "D": 18, "E": 14,
                        "F": 14, "G": 20, "H": 16, "I": 16, "J": 16,
                        "K": 16, "L": 16}.items():
            ws.column_dimensions[col].width = wd

        self._write_frame(wb, "Exceptions - no claim", self.exceptions,
                          money, base, bold)
        self._write_frame(wb, "Flagged policies", self.flagged,
                          money, base, bold)
        self._write_values(wb, money, base, bold)

        wb.save(out_path)
        print(f"Results saved to {out_path}  ({' + '.join(wb.sheetnames)})")
        return out_path

    def _write_frame(self, wb, sheet, df, money, base, bold):
        from openpyxl.styles import Border, Side
        from openpyxl.utils import get_column_letter
        ws = wb.create_sheet(sheet[:31])
        if df.empty:
            ws.cell(row=1, column=1, value="None.").font = base
            return
        for j, name in enumerate(df.columns, start=1):
            c = ws.cell(row=1, column=j, value=str(name))
            c.font = bold
            c.border = Border(bottom=Side(style="thin"))
            ws.column_dimensions[get_column_letter(j)].width = max(
                12, min(len(str(name)) + 3, 34))
        for row in df.itertuples(index=False, name=None):
            ws.append([v.item() if hasattr(v, "item") else v for v in row])
        for j, name in enumerate(df.columns, start=1):
            if name in ("beginning_fund_value", "inflow", "outflow", "exp_av",
                        "end_fund_value", "diff", "beginning_reserve_stat"):
                for rr in range(2, len(df) + 2):
                    ws.cell(row=rr, column=j).number_format = money
        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(df.columns))}{len(df) + 1}"

    def _write_values(self, wb, money, base, bold):
        h = self.headline
        ws = wb.create_sheet("Summary (values)")
        ws.sheet_view.showGridLines = False
        rows = [
            ("Policies in force", h["policies_in_force"], "#,##0"),
            ("New issues", h["new_policies"], "#,##0"),
            ("Run-off in extract", h["runoff_policies"], "#,##0"),
            ("Left the extract", h["left_extract"], "#,##0"),
            ("", None, None),
            ("Beginning AV", h["beginning_av"], money),
            ("+ Inflow", h["inflow"], money),
            ("- Outflow", h["outflow"], money),
            ("Expected end AV", h["expected_av"], money),
            ("Actual end AV", h["ending_av"], money),
            ("", None, None),
            ("Total difference", h["total_diff"], money),
            ("Left extract, no claim", h["left_no_claim_diff"], money),
            ("Left extract, with claim", h["left_with_claim_diff"], money),
            ("Reversals", h["reversal_diff"], money),
            ("TRUE UNEXPLAINED BREAK", h["unexplained_diff"], money),
            ("Policies unexplained", h["unexplained_policies"], "#,##0"),
        ]
        for i, (text, val, fmt) in enumerate(rows, start=1):
            font = bold if text.startswith(
                ("Total", "Expected", "Actual", "Policies", "TRUE")) else base
            c = ws.cell(row=i, column=1, value=text)
            c.font = font
            if val is not None:
                v = ws.cell(row=i, column=2,
                            value=float(val) if fmt == money else int(val))
                v.font = font
                v.number_format = fmt
        ws.column_dimensions["A"].width = 32
        ws.column_dimensions["B"].width = 20

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
            "break_category",
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
            if name == "date_issued":
                for r in range(2, n + 2):
                    ws.cell(row=r, column=j).number_format = "yyyy-mm-dd"
            ws.column_dimensions[letter].width = max(12, min(len(name) + 3, 30))

        ws.freeze_panes = "A2"
        ws.auto_filter.ref = f"A1:{get_column_letter(len(cols))}{n + 1}"
        return {name: (get_column_letter(j), n, sheet)
                for j, name in enumerate(cols, start=1)}


def summarize_avrf(source, set_month, threshold=FLAG_THRESHOLD,
                   rounding_tol=ROUNDING_TOL):
    """Roll an AVRF detail frame up to a management summary with a bridge."""
    df = pd.read_excel(source) if isinstance(source, str) else source.copy()
    _validate(df)

    if "status" not in df.columns or "break_category" not in df.columns:
        df = add_rollforward(df)

    diff = df["diff"]
    abs_diff = diff.abs()
    is_break = abs_diff > rounding_tol
    in_force = df["beginning_fund_value"] > 0

    n_zero_bom = int((~in_force).sum())
    if n_zero_bom:
        print(
            f"Note: {n_zero_bom} rows have beginning AV = 0 (new issues). They "
            "are excluded from the in-force count but included in the totals."
        )

    bridge = (
        df.loc[is_break]
        .groupby("break_category")
        .agg(policies=("policy_number", "size"),
             beginning_av=("beginning_fund_value", "sum"),
             diff=("diff", "sum"))
        .round(2)
    )

    def cat_sum(cat):
        m = is_break & (df["break_category"] == cat)
        return float(diff[m].sum())

    unexpl = is_break & (df["break_category"] == CAT_UNEXPLAINED)
    no_claim = df["break_category"] == CAT_LEFT_NO_CLAIM

    headline = {
        "policies_in_force": int(in_force.sum()),
        "new_policies": int((df["status"] == "New").sum()),
        "runoff_policies": int((df["status"] == "Run-off in extract").sum()),
        "left_extract": int((df["status"] == "Left extract").sum()),
        "beginning_av": float(df["beginning_fund_value"].sum()),
        "inflow": float(df["inflow"].sum()),
        "outflow": float(df["outflow"].sum()),
        "expected_av": float(df["exp_av"].sum()),
        "ending_av": float(df["end_fund_value"].sum()),
        "total_diff": float(diff[is_break].sum()),
        "total_abs_diff": float(abs_diff[is_break].sum()),
        "break_policies": int(is_break.sum()),
        "left_no_claim_diff": cat_sum(CAT_LEFT_NO_CLAIM),
        "left_with_claim_diff": cat_sum(CAT_LEFT_WITH_CLAIM),
        "reversal_diff": cat_sum(CAT_REVERSAL),
        "unexplained_diff": float(diff[unexpl].sum()),
        "unexplained_policies": int(unexpl.sum()),
        "left_no_claim_av": float(df.loc[no_claim, "beginning_fund_value"].sum()),
        "flagged_policies": int((abs_diff > threshold).sum()),
    }

    flagged = (
        df.loc[abs_diff > threshold]
        .reindex(columns=[c for c in FLAG_COLS if c in df.columns])
        .sort_values("diff", key=abs, ascending=False)
        .reset_index(drop=True)
    )
    exceptions = (
        df.loc[no_claim]
        .reindex(columns=[c for c in EXCEPTION_COLS if c in df.columns])
        .sort_values("beginning_fund_value", ascending=False)
        .reset_index(drop=True)
    )

    return AVRFSummary(
        set_month=set_month,
        threshold=threshold,
        headline=headline,
        bridge=bridge,
        flagged=flagged,
        exceptions=exceptions,
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

    if "diff" in df.columns and df["diff"].isna().any():
        raise ValueError(
            f"{int(df['diff'].isna().sum())} policies have a null 'diff'. Nulls "
            "understate the break total - fix the components first."
        )

    dupes = int(df["policy_number"].duplicated().sum())
    if dupes:
        print(
            f"WARNING: {dupes} duplicate policy_number rows will double-count "
            "in the totals. Check for multiple rows per policy in "
            "`aclico.policy` or `seriatim_values`."
        )


# ======================================================================
# MONTHLY CHECKS
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
    """Test alternative outflow definitions against the actual ending AV.

    exp_av excludes surrender_fees. Re-run whenever a month's break jumps -
    it separates a formula problem from a data problem. Restrict to policies
    present in both months, or the departures swamp the comparison.
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


def tag_recaptures(df, settlement_path):
    """Split 'left extract, no claim' into treaty recaptures and the rest.

    The recapture list lives in the settlement workbook, not in BigQuery, so
    this is a separate optional step. Recaptured policies leave because the
    cession changed (reins code P/V/AF/3C -> PR/VR/AFR/3A, and the extract
    carries no "-R" codes), not because the fund paid out - so they are a
    treaty movement, not a reconciling item.

    Adds a `recaptured` column and refines break_category in place.
    """
    rec = pd.read_excel(settlement_path, 'Converge Monthly Recapture')
    key = pd.to_numeric(
        rec['Policy ID'].astype(str).str.replace('-', '', regex=False),
        errors='coerce')
    recaptured = set(key[rec['Recapture This Month'] == 'Y'].dropna().astype('int64'))

    out = df.copy()
    out['recaptured'] = pd.to_numeric(
        out['policy_number'], errors='coerce').isin(recaptured)
    out.loc[out['recaptured'] & (out['break_category'] == CAT_LEFT_NO_CLAIM),
            'break_category'] = "A1. Left extract - treaty recapture"
    out.loc[~out['recaptured'] & (out['break_category'] == CAT_LEFT_NO_CLAIM),
            'break_category'] = "A2. Left extract - no claim, no recapture"
    print(f"  {int(out['recaptured'].sum())} policies flagged as recaptured "
          f"this month")
    return out


def status_breakdown(df):
    """Where the break sits by status - run this first when a month looks off."""
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
