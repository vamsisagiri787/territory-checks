# Territory Checks Internal Announcement Rules

## Purpose

This job does three related things:

1. `main.py`
   Reads Outlook mail, builds territory outputs, and writes internal-announcement raw/silver rows.
2. `fill_internal_announcements.py`
   Builds the balances and deposits workbook from silver.
3. `fill_balances_vs_budget.py`
   Builds the balances-vs-budget workbook from silver and the template workbook.

## Internal Announcement Sender Rules

- Internal announcements are currently tracked from:
  - `rgalloway@strategicfranchising.com`
  - `jsiehl@franchisesupport.net`
- These mails are skipped from territory-count logic and routed into the internal-announcement parsing flow.

## Raw Parsing Rules

- RE/FW mails are skipped by default.
- RE/FW mails are still allowed when they contain an embedded original template or ACTION payload.
- Threaded mails are skipped unless they contain an embedded original template or ACTION payload.
- Non-template mails are marked `NOT_TEMPLATE`.

## Template Parsing Rules

- Subject-based template types are preserved whenever possible.
- Current template types explicitly recognized:
  - `DEPOSIT`
  - `PARTIAL BALANCE`
  - `BALANCE`
  - `DEPOSIT + BALANCE`
  - `ROFR`
  - `Transfer Fee Paid`
  - `Territory Amendment`
  - `Renewal Complete`
  - `Expiration of Franchise Agreement`
- `Net Balance` is preferred over `Amount to Draft Today` for amount extraction.
- `brand` falls back to subject parsing when body parsing leaves brand blank.

## ACTION Parsing Rules

- ACTION mails are used to backfill:
  - `closed_sale_date`
  - blank `state_code`
- ACTION mails do not overwrite existing template `announcement_type`.
- ACTION mails can create rows only when no template row exists for that franchise ID.
- ACTION types currently recognized:
  - `Transfer Complete`
  - `Transfer In Progress`
  - `Training Approved/Deal Closed`
  - `Deal Closed`
  - `Additional Franchise Purchase Complete`
  - `Renewal Complete`
  - `Expiration of Franchise Agreement`
  - `Termination of Franchise Agreement`
  - `Mutual Termination`

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
- For workbook month placement:
  - use `closed_sale_date` month when it exists
  - otherwise use `balance_deposit_date`

## ROFR Rules

- ROFR template rows are kept.
- ROFR ACTION mails are intentionally skipped from silver action mapping.
- ROFR payment-style template rows can still appear in silver/workbook as template rows.

## Non-Financial ACTION Rules

- These ACTION mails are kept in raw/audit only and are intentionally skipped from
  silver/final balances-deposits output:
  - `ROFR` ACTION mails
  - `Renewal Complete`
  - `Expiration of Franchise Agreement`
- Reason:
  - they are operational/legal workflow mails
  - they are not balance/deposit financial rows
  - they should not drive month placement in the balances/deposits workbook

## Multi-ID Rules

- Transfer-style and transfer-fee mails can contain multiple seller franchise IDs.
- One clean template row is created for each franchise ID when multiple IDs are detected.
- This prevents later ACTION mails from appearing as separate action-only rows with the wrong type.

## Name Rules

- Correction replies such as `The new Franchisee's are ...` override earlier name values.
- For `Transfer Fee Paid`, multiple `Name` lines are combined into one `franchisee_name` value so seller and buyer context is not lost.

## Silver Merge Rules

- Silver is canonicalized by merge key:
  - `franchisee_id`
  - otherwise `franchisee_name`
  - otherwise `raw_id`
- For grouped rows:
  - last non-empty value wins
  - concrete `announcement_type` wins over blank/`OTHER`
- ACTION merge into silver:
  - updates matching non-ACTION rows with latest `closed_sale_date`
  - backfills blank `state_code` and blank `franchisee_name`
  - deletes matched ACTION-only rows when a non-ACTION row already exists

## Workbook Rules

- `fill_internal_announcements.py` builds brand tabs from silver.
- Rows with `brand IS NULL` will not land in a brand tab, so subject-brand fallback in `main.py` is required.
- Month sectioning uses:
  - `closed_sale_date` month first
  - `balance_deposit_date` month second
