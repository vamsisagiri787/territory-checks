# Territory Checks And Internal Announcement Runbook

## Purpose

This project has three separate responsibilities:

1. `main.py`
   Reads Outlook mail, builds territory outputs, writes internal-announcement bronze/silver rows, and performs silver merge/backfill logic.
2. `fill_internal_announcements.py`
   Builds the Balances and Deposits workbook from `silver.sfs_internal_announcements`.
3. `fill_balances_vs_budget.py`
   Builds the Balances vs Budget workbook from silver and the summary template.

The recommended operating model is:

- `Cloud Scheduler`
- `Cloud Workflows`
- `sfs-territory-checks-job`
- `sfs-internal-announcements-job`
- `sfs-balances-vs-budget-job`

## Internal Announcement Sender Rules

- Internal announcements are currently tracked from:
  - `rgalloway@strategicfranchising.com`
  - `jsiehl@franchisesupport.net`
- These mails are skipped from territory-count logic and routed into the internal-announcement parsing flow.
- Bronze should capture all relevant template and action emails. Silver then applies business rules and canonicalization.

## Raw Parsing Rules

- RE/FW mails are skipped by default.
- RE/FW mails are still allowed when they contain an embedded original template or ACTION payload.
- Threaded mails are skipped unless they contain an embedded original template or ACTION payload.
- Non-template mails are marked `NOT_TEMPLATE` in bronze.

## Template Parsing Rules

- Subject-based template types are preserved whenever possible.
- Current template types explicitly recognized:
  - `DEPOSIT`
  - `PARTIAL BALANCE`
  - `BALANCE`
  - `DEPOSIT + BALANCE`
  - `DEPOSIT + PARTIAL BALANCE`
  - `ROFR`
  - `Transfer Fee Paid`
  - `Territory Amendment`
  - `Renewal Complete`
  - `Expiration of Franchise Agreement`
- `Net Balance` is preferred over `Amount to Draft Today` for amount extraction.
- `brand` falls back to subject parsing when body parsing leaves brand blank.
- Distinct financial rows for the same franchise ID are preserved when date/amount/type differ.

## ACTION Parsing Rules

- ACTION mails are used to backfill:
  - `closed_sale_date`
  - blank `state_code`
  - richer `franchisee_name` when seller/buyer context is better
- ACTION mails do not overwrite existing template `announcement_type`.
- ACTION mails can create rows only when no template row exists for that franchise ID.
- ACTION types currently recognized:
  - `Transfer Complete`
  - `Transfer In Progress`
  - `Training Approved`
  - `Training Approved/Deal Closed`
  - `Deal Closed`
  - `Additional Franchise Purchase Complete`
  - `Renewal Complete`
  - `Expiration of Franchise Agreement`
  - `Termination of Franchise Agreement`
  - `Mutual Termination`
- Worst-case fallback:
  - if an action-like mail still lands in silver as `OTHER`, but has:
    - `closed_sale_date`
    - no `balance_deposit_date`
    - no `amount_usd`
  - then it is still eligible to merge onto the canonical financial row by `brand + franchisee_id`.

## Date Rules

- `closed_sale_date` is taken from explicit ACTION/template effective dates when present.
- Supported effective date labels include:
  - `TODAY'S DATE`
  - `TRAINING APPROVED DATE`
  - `Transfer Closing Date`
  - `Transfer Effective Date`
  - `Franchise Agreement-Effective Date`
  - `Closed Sale Date`
  - `Date Renewal Effective Date`
  - `Date Franchise Expiration Effective`
  - `Date Mutual Termination Effective`
- Action-date extraction tries candidate fields in order and returns the first parsable date. This avoids placeholder values like `TBD 2026` blocking a valid later date in the same email.
- For workbook month placement:
  - use `closed_sale_date` month when it exists
  - otherwise use `balance_deposit_date`

## Name Rules

- Correction replies such as `The new Franchisee's are ...` override earlier name values.
- For transfer-style rows, seller and buyer names should be preserved whenever possible.
- `Transfer Fee Paid` rows should keep seller and buyer context together in `franchisee_name`.
- Action backfill may upgrade a name to a richer seller/buyer version.

## Multi-ID Rules

- Multi-location transfer/transfer-fee emails can reference several franchise IDs in one payment/action chain.
- The intended silver/reporting behavior is one canonical financial row when the email represents one combined financial event.
- Combined rows can still receive ACTION backfill from later single-ID action emails.

## Silver Merge Rules

- Silver is canonicalized by merge key:
  - `franchisee_id`
  - otherwise `franchisee_name`
  - otherwise `raw_id`
- Distinct financial events keep separate merge keys when date/amount/type differ.
- Concrete template `announcement_type` wins over blank or `OTHER`.
- ACTION merge into silver:
  - updates matching financial/core rows with latest `closed_sale_date`
  - backfills blank `state_code`
  - backfills richer `franchisee_name`
  - works across run slices, not only within the current week
  - can fall back to a strong `brand + franchisee_name` match when `franchisee_id` is missing or not usable in the action row
  - unmatched action rows can still remain visible for manual review when no confident silver match exists
  - deletes matched action-only residue rows when a canonical financial/core row already exists

## Workbook Rules

- `fill_internal_announcements.py` builds brand tabs from silver.
- Workbook outputs now follow the same weekly folder pattern used by territory checks:
  - `territory-checks/weekly/YYYY/MM/...`
  - `sfs_strategic_franchising/Balances and Deposits/outputs/weekly/YYYY/MM/...`
  - `sfs_strategic_franchising/Balances_VS_Budget/outputs/weekly/YYYY/MM/...`
- This keeps monthly bucket browsing consistent across all generated files.
- Rows with `brand IS NULL` will not land in a brand tab, so subject-brand fallback in `main.py` is required.
- Workbook month sectioning uses:
  - `closed_sale_date` month first
  - `balance_deposit_date` month second
- Workbook dedupe keeps rows distinct when date/amount/type differ.
- Residual `OTHER` rows with only action-style `closed_sale_date` are suppressed when a canonical financial row already exists.

## Balances Vs Budget Rules

- `fill_balances_vs_budget.py` counts only top-section balance rows:
  - `BALANCE`
  - `DEPOSIT + BALANCE`
- It does not count:
  - `Transfer Fee Paid`
  - `Territory Amendment`
  - `PARTIAL BALANCE`
  - other non-top-section types

## Cloud Run Jobs

Expected jobs:

- `sfs-territory-checks-job`
  - command: `python3 /app/main.py`
- `sfs-internal-announcements-job`
  - command: `python3 /app/fill_internal_announcements.py`
- `sfs-balances-vs-budget-job`
  - command: `python3 /app/fill_balances_vs_budget.py`

Recommended shared settings:

- image: current production tag in Artifact Registry
- service account: `run-sa@sfs-data-lake.iam.gserviceaccount.com`
- env vars:
  - `GCS_BUCKET=sfs-raw-us`
  - `LOGO_DIR=/app/logos`
- secrets:
  - `GRAPH_CLIENT_ID`
  - `GRAPH_CLIENT_SECRET`
  - `GRAPH_TENANT_ID`

## Workflow Deployment

Workflow file:

- `weekly_reporting_workflow.yaml`

Deploy workflow:

```bash
gcloud workflows deploy sfs-weekly-reporting-workflow --source=weekly_reporting_workflow.yaml --location=us-central1 --project=sfs-data-lake
```

Manual workflow test:

```bash
gcloud workflows run sfs-weekly-reporting-workflow --location=us-central1 --project=sfs-data-lake
```

## CLI Component Notes

Some `gcloud` commands used in this project are not part of the smallest default CLI install.
That is why Google may prompt to install extra components the first time you run them.

Typical examples in this runbook:

- `gcloud beta monitoring channels list`
  - uses the `beta` component
- `gcloud alpha monitoring policies create`
  - uses the `alpha` component

Why this happens:

- `gcloud` has stable core commands
- some Monitoring/alert-policy commands still live under `alpha` or `beta`
- Google prompts to install those components the first time they are used on a machine

Safe install commands:

```bash
gcloud components install beta
gcloud components install alpha
```

If a team member does not want to install `alpha`/`beta`, the fallback is:

- create the email channel in Console
- create the alert policy in Console
- keep workflow/job deployment in CLI

## Alerting Before Scheduling

Recommended production sequence:

1. Add workflow failure logging in `weekly_reporting_workflow.yaml` using `try/except` and `sys.log`.
2. Redeploy the workflow.
3. Create a Monitoring email notification channel.
4. Create a log-based alert on workflow failure events.
5. Test one manual workflow run.
6. Only then enable the scheduler.

CLI sequence for alerting:

1. List email channels:

```bash
gcloud beta monitoring channels list --project=sfs-data-lake --format="table(name,displayName,type,labels.email_address)"
```

2. Create the alert policy:

```bash
gcloud alpha monitoring policies create --policy-from-file=alert-policy.json --project=sfs-data-lake
```

Notes:

- the first `beta`/`alpha` command may ask to install extra CLI components
- answer `Y` if you want to use the CLI path on that machine
- this is normal and expected

Recommended custom log event name:

- `SFS_WEEKLY_REPORTING_FAILED`

Recommended Monitoring filter:

```text
resource.type="workflows.googleapis.com/Workflow"
logName="projects/sfs-data-lake/logs/Workflows"
severity>=ERROR
jsonPayload.event="SFS_WEEKLY_REPORTING_FAILED"
```

## Scheduler

Create scheduler only after workflow + alerting are validated:

```bash
gcloud scheduler jobs create http sfs-weekly-reporting-scheduler --location=us-central1 --schedule="0 8 * * 1" --time-zone="America/New_York" --uri="https://workflowexecutions.googleapis.com/v1/projects/sfs-data-lake/locations/us-central1/workflows/sfs-weekly-reporting-workflow/executions" --http-method=POST --headers="Content-Type=application/json,User-Agent=Google-Cloud-Scheduler" --message-body="{}" --oauth-service-account-email="run-sa@sfs-data-lake.iam.gserviceaccount.com" --project=sfs-data-lake
```

If an older scheduler still points to the old combined job, disable or repoint it after cutover.

## Build And Update Jobs

Example image rollout:

```bash
gcloud builds submit --tag us-central1-docker.pkg.dev/sfs-data-lake/territory-checks-repo/territory-checks:replies-v29 --project sfs-data-lake && gcloud run jobs update sfs-territory-checks-job --image us-central1-docker.pkg.dev/sfs-data-lake/territory-checks-repo/territory-checks:replies-v29 --region us-central1 --project sfs-data-lake && gcloud run jobs update sfs-internal-announcements-job --image us-central1-docker.pkg.dev/sfs-data-lake/territory-checks-repo/territory-checks:replies-v29 --region us-central1 --project sfs-data-lake && gcloud run jobs update sfs-balances-vs-budget-job --image us-central1-docker.pkg.dev/sfs-data-lake/territory-checks-repo/territory-checks:replies-v29 --region us-central1 --project sfs-data-lake
```

## Rerun Strategy

- If ingestion fails:
  - fix `main.py` / source issue
  - rerun workflow or just `sfs-territory-checks-job`
- If silver is manually corrected:
  - rerun only `fill_internal_announcements.py` and/or `fill_balances_vs_budget.py`
- If workbook output is wrong but silver is correct:
  - do not rerun ingestion unless needed
  - rerun the report job only

## Validation Queries

Typical silver validation pattern:

```sql
SELECT
  run_date_from,
  run_date_to,
  franchisee_id,
  franchisee_name,
  announcement_type,
  balance_deposit_date,
  closed_sale_date,
  amount_usd,
  received_datetime,
  raw_id
FROM `sfs-data-lake.silver.sfs_internal_announcements`
WHERE franchisee_id IN ('25596','35946','36229')
ORDER BY franchisee_id, run_date_from, received_datetime;
```

Typical bronze validation pattern:

```sql
SELECT
  received_datetime,
  sender_email,
  subject,
  skipped_reason,
  extracted_fields_json,
  raw_id
FROM `sfs-data-lake.bronze.sfs_internal_announcements_raw`
WHERE LOWER(subject) LIKE '%25596%'
   OR LOWER(subject) LIKE '%35946%'
   OR LOWER(subject) LIKE '%36229%'
ORDER BY received_datetime;
```
