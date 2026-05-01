#!/usr/bin/env python3
"""
SILAC settlement ETL converted from notebook to a single Python script.

What it does
------------
1. Reads the settlement workbook sheets:
   - Seriatim
   - Withdrawals
   - Premiums
   - Notional
   - Commissions
   - Monthly Deaths
2. Cleans and standardizes columns
3. Builds table-specific dataframes
4. Uploads to BigQuery
5. Runs validation queries
6. Optionally runs AVRF, reconciliation, and LDTI downstream jobs

Usage
-----
python run_etl.py \
  --input "C:/path/to/202601 Converge Teton Settlement file.xlsx" \
  --creds "../converge-database-0331482f2ee5.json"

Optional:
  --skip-upload
  --skip-tests
  --skip-post
  --output-dir "./output"
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

try:
    from google.cloud import bigquery
except Exception:  # pragma: no cover
    bigquery = None


CREDIT_DICTIONARY = {
    "Barclays Atlas 5 Point-to-Point PR": "ATLAS-AP2PPR",
    "S&P 500 RavenPack AI Point-to-Point PR": "RVP-AP2PPR",
    "S&P 500 Monthly Average PR": "MAPR",
    "S&P 500 Monthly Point-to-Point Cap": "MP2PC",
    "S&P 500 Point-to-Point Cap": "AP2PC",
    "S&P 500 Point-to-Point PR": "AP2PPR",
    "NDX Generations 5 Point-to-Point PR": "NASDAQ-AP2PPR",
    "Barclays Atlas 5 Point-to-Point Spread": "ATLAS-AP2PS",
    "Fixed Interest": "FI",
    "NDX Generations 5 Point-to-Point Spread": "NASDAQ-AP2PS",
    "S&P 500 RavenPack AI Point-to-Point Spread": "RVP-AP2PS",
    "S&P 500 Monthly Average Cap": "MAC",
    "S&P 500 Monthly Average Spread": "MAS",
    "S&P 500 Duo Swift Point-to-Point PR": "SPDS-AP2PPR",
    "CS RavenPack AI Point-to-Point PR": "RVP-AP2PPR",
    "CS RavenPack AI Point-to-Point Spread": "RVP-AP2PS",
    "S&P 500 RavenPack AI Point-to-Point Sprd": "RVP-AP2PS",
}


def setup_logging(log_dir: Path) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"silac_etl_{datetime.now():%Y%m%d_%H%M%S}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file, encoding="utf-8"), logging.StreamHandler(sys.stdout)],
    )
    logging.info("Logging started: %s", log_file)


def clean_columns(df: pd.DataFrame, *, space_replacement: str = "", replace_parens: bool = False) -> pd.DataFrame:
    cols = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .map(lambda x: x.replace(" ", space_replacement))
    )
    if replace_parens:
        cols = cols.map(lambda x: x.replace("(", "_")).map(lambda x: x.replace(")", "_"))
    df = df.copy()
    df.columns = cols
    return df


def safe_cast(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    for col, dtype in mapping.items():
        if col not in df.columns:
            continue
        try:
            if dtype.startswith("datetime64"):
                df[col] = pd.to_datetime(df[col], errors="coerce")
            elif dtype in {"int", "int64", "Int64"}:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
            elif dtype in {"float", "float64"}:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            elif dtype == "string":
                df[col] = df[col].astype("string")
            else:
                df[col] = df[col].astype(dtype)
        except Exception as exc:
            logging.warning("Failed casting column %s to %s: %s", col, dtype, exc)
    return df


def load_workbook(file_path: Path) -> Dict[str, pd.DataFrame]:
    logging.info("Reading workbook: %s", file_path)
    sheets = {
        "seriatim": pd.read_excel(file_path, sheet_name="Seriatim"),
        "withdrawals": pd.read_excel(file_path, sheet_name="Withdrawals"),
        "premiums": pd.read_excel(file_path, sheet_name="Premiums"),
        "notional": pd.read_excel(file_path, sheet_name="Notional"),
        "commissions": pd.read_excel(file_path, sheet_name="Commissions"),
        "deaths": pd.read_excel(file_path, sheet_name="Monthly Deaths"),
    }
    logging.info("Workbook loaded successfully.")
    return sheets


def prepare_frames(sheets: Dict[str, pd.DataFrame]) -> Tuple[Dict[str, pd.DataFrame], str, str]:
    seriatim = clean_columns(sheets["seriatim"], space_replacement="_")
    withdrawals = clean_columns(sheets["withdrawals"], replace_parens=True)
    premiums = clean_columns(sheets["premiums"])
    notional = clean_columns(sheets["notional"])
    commissions = clean_columns(sheets["commissions"])
    deaths = clean_columns(sheets["deaths"])

    seriatim["credit_id"] = seriatim.get("creditstrategy", pd.Series(index=seriatim.index)).map(CREDIT_DICTIONARY)

    policy = seriatim.iloc[:, 0:19].copy()
    policy["credit_id"] = policy.get("creditstrategy", pd.Series(index=policy.index)).map(CREDIT_DICTIONARY)
    if "creditstrategy" in seriatim.columns:
        policy["creditstrategy"] = seriatim["creditstrategy"]
    if "rider_spread" in policy.columns:
        policy = safe_cast(policy, {"rider_spread": "int64"})

    seriatim = safe_cast(
        seriatim,
        {
            "mtd_index_interest": "float64",
            "currentbonusrecoverypercentage": "float64",
            "strategy_ytd_nursing_care_withdrawals": "float64",
            "strategy_ytd_home_health_care_withdrawals": "float64",
            "policy_ytd_cumulative_withdrawal": "int64",
            "policy_ytd_nursing_care_withdrawals": "float64",
            "policy_ytd_home_health_care_withdrawals": "float64",
            "total_cumulative_withdrawal": "int64",
            "total_nursing_care_withdrawals": "float64",
            "total_home_health_care_withdrawals": "float64",
            "strategy_ytd_terminal_illness_withdrawals": "float64",
            "strategy_ytd_cumulative_withdrawal": "int64",
            "total_terminal_illness_withdrawals": "float64",
            "policy_ytd_terminal_illness_withdrawals": "float64",
        },
    )

    seriatim = seriatim.drop(
        [
            "product",
            "plan",
            "rateversion",
            "stateofsale",
            "taxqualstatus",
            "issueyear",
            "issuemonth",
            "issueday",
            "issueage",
            "ownerresidentstate",
            "ownergender",
            "annuitant_issue_age",
            "annuitant_gender",
            "joint_annuitant_issue_age",
            "joint_annuitant_gender",
            "riders",
            "rider_spread",
        ],
        axis=1,
        inplace=False,
        errors="ignore",
    )
    seriatim = seriatim.rename(columns={"joint/single_payment": "joint_single_payment"})

    withdrawals = withdrawals.drop(
        ["policyid", "product", "plan", "approvaldate", "policyissuestate"],
        axis=1,
        errors="ignore",
    )
    premiums = premiums.drop(
        ["policyid", "product", "approvaldate", "policyissuestate", "ownerissueage"],
        axis=1,
        errors="ignore",
    )
    notional = notional.drop(
        ["polid", "product_conf", "rateversion", "product", "planlength", "state", "creditingstrategyname"],
        axis=1,
        errors="ignore",
    )
    commissions = commissions.drop(
        ["policyid", "product", "plan", "approvaldate", "policyissuestate"],
        axis=1,
        errors="ignore",
    )
    deaths = deaths.drop(["product", "plan", "withdrawaltype"], axis=1, errors="ignore")

    withdrawals = safe_cast(
        withdrawals,
        {
            "totalpolicypremiumreceived": "float64",
            "transactiondate": "datetime64[ns]",
        },
    )
    withdrawals = withdrawals.iloc[:, 0:36].copy()
    if "policynumber" in withdrawals.columns:
        withdrawals = withdrawals.dropna(subset=["policynumber"])

    if "transactiondate" not in withdrawals.columns or withdrawals["transactiondate"].dropna().empty:
        raise ValueError("Could not derive set_month because withdrawals.transactiondate is missing or empty.")
    set_month = withdrawals["transactiondate"].dropna().iloc[0].strftime("%Y%m")

    if "policynumber" not in policy.columns or policy["policynumber"].dropna().empty:
        raise ValueError("Could not derive product because policy.policynumber is missing or empty.")
    first_policy = str(policy["policynumber"].dropna().iloc[0])
    product_like = f"{first_policy[0]}%"

    seriatim = seriatim.rename(columns={"converge_-_teton": "converge"})
    withdrawals = withdrawals.drop(["converge-denali"], axis=1, errors="ignore")
    commissions = commissions.drop(["converge-denali"], axis=1, errors="ignore")
    premiums = premiums.drop(["converge-denali"], axis=1, errors="ignore")
    notional = notional.drop(["converge-denali"], axis=1, errors="ignore")

    seriatim = seriatim.rename(
        columns={
            "payout/lifetime_withdrawal_type": "joint_single_payment",
            "bonus_%": "bonus_percent",
            "lifetimelevsingle50": "lifetimesingle50",
            "lifetimelevsingle60": "lifetimesingle60",
            "lifetimelevsingle70": "lifetimesingle70",
            "lifetimelevsingle80": "lifetimesingle80",
            "lifetimelevjoint50": "lifetimejoint50",
            "lifetimelevjoint60": "lifetimejoint60",
            "lifetimelevjoint70": "lifetimejoint70",
            "lifetimelevjoint80": "lifetimejoint80",
            "total_premium_bonus_%": "total_premium_bonus_percent",
            "lifetime_withdrawals_elected_date": "lifetimewithdrawalselecteddate",
            "wellness_withdrawals_elected_date": "wellnesswithdrawalselecteddate",
            "wellness_withdrawals_termination_date": "wellnesswithdrawalstermdate",
        }
    )

    if product_like == "D%":
        seriatim = safe_cast(
            seriatim,
            {
                "lifetimewithdrawalselecteddate": "datetime64[ns]",
                "wellnesswithdrawalselecteddate": "datetime64[ns]",
                "wellnesswithdrawalstermdate": "datetime64[ns]",
            },
        )

    seriatim = seriatim.iloc[:, 0:87].copy()

    notional = notional.dropna(subset=["policynumber"], axis=0) if "policynumber" in notional.columns else notional
    notional = notional.iloc[:, 0:12].copy()

    withdrawals = safe_cast(withdrawals, {"termdate": "datetime64[ns]"})

    premiums = premiums.dropna(subset=["policynumber"], axis=0) if "policynumber" in premiums.columns else premiums
    premiums = premiums.iloc[:, 0:17].copy()
    premiums = safe_cast(
        premiums,
        {
            "premiumrecognitionyyyymm": "int64",
            "terminateddate": "datetime64[ns]",
            "addtlpremrcvddate": "datetime64[ns]",
            "deleteddate": "datetime64[ns]",
            "premiumrecognitionmonth": "datetime64[ns]",
            "plan": "string",
        },
    )

    commissions = commissions.iloc[:, 0:13].copy()
    commissions = commissions.dropna(subset=["policynumber"], axis=0) if "policynumber" in commissions.columns else commissions
    commissions = safe_cast(
        commissions,
        {
            "commissionrecognitionyyyymm": "int64",
            "issuedate": "datetime64[ns]",
            "agentnumber": "int64",
            "enterdate": "datetime64[ns]",
            "commissionrecognitionmonth": "datetime64[ns]",
        },
    )
    commissions = commissions.drop(["rider"], axis=1, errors="ignore")

    deaths = deaths.rename(columns={"reinsurancecode": "reins"})
    deaths = safe_cast(deaths, {"converge": "float64", "reins": "string"})
    deaths = deaths.iloc[:, 0:5].copy()
    deaths = deaths.dropna(subset=["policynumber"], axis=0) if "policynumber" in deaths.columns else deaths
    deaths = deaths.drop(["rider"], axis=1, errors="ignore")

    frames = {
        "policy": policy,
        "seriatim_values": seriatim,
        "notional": notional,
        "withdrawals": withdrawals,
        "premiums": premiums,
        "commissions": commissions,
        "deaths": deaths,
    }
    
    return frames, set_month, product_like


def get_bq_client(creds_path: Path):
    if bigquery is None:
        raise ImportError("google-cloud-bigquery is not installed.")
    return bigquery.Client.from_service_account_json(json_credentials_path=str(creds_path))

def upload_table(df: pd.DataFrame, table_name: str, set_month: str, file_path: Path, project_id: str) -> None:
    upload_df = df.copy()
    if table_name not in {"premiums", "commissions", "notional"}:
        upload_df["set_month"] = set_month

    logging.info("Uploading %s (%s rows)", table_name, len(upload_df))
    start = time.time()
    upload_df.to_gbq(f"{project_id}.denali.{table_name}", if_exists="append", project_id=project_id)

    elapsed = time.time() - start
    logging.info("Upload complete for %s in %.2f seconds", table_name, elapsed)


def run_validation_test(
    client,
    table: str,
    set_month: str,
    product_like: str,
    frames: Dict[str, pd.DataFrame],
    project_id: str,
) -> str:
    logging.info("Running validation for %s | set_month=%s | product=%s", table, set_month, product_like)

    if table == "policy":
        query = (
            f'SELECT count(*) FROM `denali.policy` '
            f'WHERE set_month="{set_month}" AND policynumber LIKE "{product_like}"'
        )
        excel_result = frames["policy"]["policynumber"].count()
    elif table == "seriatim":
        query = (
            f'SELECT sum(reserves2) FROM `denali.seriatim_values` '
            f'WHERE set_month="{set_month}" AND policynumber LIKE "{product_like}"'
        )
        excel_result = frames["seriatim_values"]["reserves2"].sum()
    elif table == "premiums":
        query = (
            f'SELECT sum(totaladdtlpremium) FROM `denali.premiums` '
            f'WHERE CAST(premiumrecognitionyyyymm AS STRING)="{set_month}" '
            f'AND policynumber LIKE "{product_like}"'
        )
        excel_result = frames["premiums"]["totaladdtlpremium"].sum()
    elif table == "withdrawals":
        query = (
            f'SELECT sum(fullsurrenders) FROM `denali.withdrawals` '
            f'WHERE set_month="{set_month}" AND policynumber LIKE "{product_like}"'
        )
        excel_result = frames["withdrawals"]["fullsurrenders"].sum()
    elif table == "notional":
        query = (
            f'SELECT sum(reallocamount) FROM `denali.notional` '
            f'WHERE FORMAT_TIMESTAMP("%Y%m", trandate)="{set_month}" '
            f'AND policynumber LIKE "{product_like}"'
        )
        excel_result = frames["notional"]["reallocamount"].sum()
    elif table == "commissions":
        query = (
            f'SELECT sum(commissionamount) FROM `denali.commissions` '
            f'WHERE CAST(commissionrecognitionyyyymm AS STRING)="{set_month}" '
            f'AND policynumber LIKE "{product_like}"'
        )
        excel_result = frames["commissions"]["commissionamount"].sum()
    elif table == "deaths":
        query = (
            f'SELECT sum(totaldeath) FROM `denali.deaths` '
            f'WHERE set_month="{set_month}" AND policynumber LIKE "{product_like}"'
        )
        excel_result = frames["deaths"]["totaldeath"].sum()
    else:
        raise ValueError(f"Unsupported validation table: {table}")

    logging.info("Validation query: %s", query)
    result_iter = client.query(query).result()
    result = next(iter(result_iter))[0]

    if round(float(result or 0), 2) == round(float(excel_result or 0), 2):
        outcome = "Pass"
    else:
        outcome = "Fail"

    logging.info(
        "%s validation => database=%s | excel=%s | result=%s",
        table,
        round(float(result or 0), 2),
        round(float(excel_result or 0), 2),
        outcome,
    )
    return outcome


def export_local_copies(frames: Dict[str, pd.DataFrame], output_dir: Path, set_month: str) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    for table_name, df in frames.items():
        out_file = output_dir / f"{table_name}_{set_month}.xlsx"
        df.to_excel(out_file, index=False)
        logging.info("Wrote local export: %s", out_file)


def run_post_steps(set_month: str, product_like: str) -> None:
    # AVRF
    try:
        sys.path.append(".")
        import avrf_analysis_silac  # type: ignore

        avrf_analysis_silac.run_avrf_analysis(set_month, product_like)
        logging.info("AVRF analysis complete.")
    except Exception as exc:
        logging.warning("Skipping AVRF analysis: %s", exc)

    # Reconciliation
    try:
        sys.path.append("../actuarial-pipelines/reconciliations/silac/")
        from reconciliation import run_reconciliation  # type: ignore

        run_reconciliation(set_month)
        logging.info("Reconciliation complete.")
    except Exception as exc:
        logging.warning("Skipping reconciliation: %s", exc)

    # LDTI
    try:
        sys.path.append("../actuarial-pipelines/ldti/")
        from LDTI import main_query_run  # type: ignore

        main_query_run("SILAC")
        logging.info("LDTI complete.")
    except Exception as exc:
        logging.warning("Skipping LDTI: %s", exc)


def update_reported_date(client) -> None:
    query_report = """
    UPDATE `denali.policy` p1
    SET p1.reported_date = (
        SELECT MIN(p2.set_month)
        FROM `denali.policy` p2
        WHERE p2.policynumber = p1.policynumber
    )
    WHERE p1.reported_date IS NULL
    """
    client.query(query_report).result()
    logging.info("Reported date update complete.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the SILAC settlement ETL.")
    parser.add_argument("--input", required=True, help="Full path to the input Excel workbook.")
    parser.add_argument("--creds", required=True, help="Path to the GCP service account JSON file.")
    parser.add_argument("--project-id", default="converge-database", help="BigQuery project ID.")
    parser.add_argument("--output-dir", default="output", help="Directory for local Excel exports.")
    parser.add_argument("--log-dir", default="logs", help="Directory for log files.")
    parser.add_argument("--skip-upload", action="store_true", help="Prepare dataframes but do not upload to BigQuery.")
    parser.add_argument("--skip-tests", action="store_true", help="Skip validation queries.")
    parser.add_argument("--skip-post", action="store_true", help="Skip AVRF, reconciliation, and LDTI steps.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    file_path = Path(args.input)
    creds_path = Path(args.creds)
    output_dir = Path(args.output_dir)
    log_dir = Path(args.log_dir)

    setup_logging(log_dir)

    if not file_path.exists():
        raise FileNotFoundError(f"Input workbook not found: {file_path}")
    if not creds_path.exists():
        raise FileNotFoundError(f"Credentials file not found: {creds_path}")

    sheets = load_workbook(file_path)
    frames, set_month, product_like = prepare_frames(sheets)
    logging.info("Derived set_month=%s | product_like=%s", set_month, product_like)

    #export_local_copies(frames, output_dir=output_dir, set_month=set_month)

    if args.skip_upload:
        logging.info("Skipping upload by request.")
        return

    client = get_bq_client(creds_path)

    upload_order = [
        "policy",
        "seriatim_values",
        "notional",
        "withdrawals",
        "premiums",
        "commissions",
        "deaths",
    ]


    for table_name in upload_order:
        upload_table(
            df=frames[table_name],
            table_name=table_name,
            set_month=set_month,
            file_path=file_path,
            project_id=args.project_id,
        )

    if not args.skip_tests:
        validation_order = ["policy", "seriatim", "premiums", "withdrawals", "notional", "commissions", "deaths"]
        results = {
            table: run_validation_test(
                client=client,
                table=table,
                set_month=set_month,
                product_like=product_like,
                frames=frames,
                project_id=args.project_id,
            )
            for table in validation_order
        }
        logging.info("Validation summary: %s", results)

    update_reported_date(client)

    if not args.skip_post:
        run_post_steps(set_month=set_month, product_like=product_like)


if __name__ == "__main__":
    main()
