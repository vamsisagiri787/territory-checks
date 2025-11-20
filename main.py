from __future__ import annotations
# ==============================================================================
#  Author : Vamsi Krishna S. (enterprise build)
#  Program: Territory Checks – Weekly Brand × Broker Counter (Sun→Sat)
#  Build  : 2.7 (enterprise, 2025-11-18 with BigQuery bronze loads)
#--------------------------------------------------------------
#  WHAT'S IN THIS BUILD
#  --------------------
#  • Week window = previous Sun 00:00:00 → this Sun 00:00:00 (UTC, exclusive)
#  • Brand detection prefers To/Cc/Bcc, then routing headers only (no noise)
#  • Growth Coach alias includes growthcoach.com
#  • Broker detection: subject → forwarded body header → sender domain
#  • Territory: subject → bodyPreview → full HTML body (fallback)
#  • De-dup: Brand × Broker × Territory (keeps first)
#  • Always creates per-brand sheets even if weekly count is 0
#  • Unmapped + Audit tabs with helpful columns
#  • “Reply in Chat” follow-ups are skipped from counts but visible in Audit
#  • Writes 3 bronze tables in BigQuery:
#       - bronze.territory_checks_raw
#       - bronze.unmapped_territory_checks
#       - bronze.audit_territory_checks
#  • No secrets in code (all from env)
# ==============================================================================

# ================================= Imports ====================================
import os
import re
import time
import json
import logging
import unicodedata
import html
import csv

from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Tuple, Optional

import requests
import msal
import pandas as pd
from openpyxl import Workbook, load_workbook
from google.cloud import storage, bigquery

# =============================== Configuration ================================
gv_TENANT_ID      = os.getenv("GRAPH_TENANT_ID", "")
gv_CLIENT_ID      = os.getenv("GRAPH_CLIENT_ID", "")
gv_CLIENT_SECRET  = os.getenv("GRAPH_CLIENT_SECRET", "")
gv_MASTER_MAILBOX = os.getenv("MASTER_MAILBOX", "territorycheck@strategicfranchising.com")
gv_OUT_DIR        = os.getenv("OUT_DIR", "/tmp")
gv_LOG_LEVEL      = os.getenv("LOG_LEVEL", "INFO").upper().strip()

# Where to put files in GCS
gv_GCS_BUCKET       = os.getenv("GCS_BUCKET", "")   # e.g. sfs-territory-raw
gv_GCS_PREFIX_EXCEL = os.getenv("GCS_PREFIX_EXCEL", "territory/excel")
gv_GCS_PREFIX_RAW   = os.getenv("GCS_PREFIX_RAW", "territory/raw")

# ---------------- BigQuery config (bronze layer) ----------------
gv_BQ_PROJECT        = os.getenv("BQ_PROJECT", "sfs-data-lake")
gv_BQ_DATASET_BRONZE = os.getenv("BQ_DATASET_BRONZE", "bronze")

gv_BQ_TABLE_TERRITORY = os.getenv("BQ_TABLE_TERRITORY", "territory_checks_raw")
gv_BQ_TABLE_UNMAPPED  = os.getenv("BQ_TABLE_UNMAPPED", "unmapped_territory_checks")
gv_BQ_TABLE_AUDIT     = os.getenv("BQ_TABLE_AUDIT", "audit_territory_checks")

gv_COUNT_FORWARDS          = True   # if False, forwards are skipped from counts
gv_SKIP_REPLIES            = True   # if True, "Re:" thread replies are skipped
gv_SHOW_SUBJECT_IN_DETAILS = True   # show Subject on brand sheets

gv_USER_AGENT   = "TerritoryChecks/2.7"
gv_HTTP_TIMEOUT = 60
gv_MAX_RETRIES  = 5

logging.basicConfig(
    level=getattr(logging, gv_LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)
gv_LOG = logging.getLogger("territory-checks")

# ================================ Brand Rules =================================
gv_BRAND_ADDRESSES: Dict[str, List[str]] = {
    "Caring Transitions": ["territorycheck@caringtransitions.com", "caringtransitions.com"],
    "Fresh Coat"        : ["territorycheck@freshcoatpainters.com",
                           "freshcoatpainters.com",
                           "freshcoat.com"],
    "TruBlue"           : ["territorycheck@trublueally.com", "trublueally.com"],
    "Growth Coach"      : ["territorycheck@thegrowthcoach.com",
                           "thegrowthcoach.com",
                           "growthcoach.com"],
    "Pet Wants"         : ["territorycheck@petwants.com", "petwants.com"],
}

# ============================== Broker Mapping ================================
gv_BROKER_SUBJECT_KEYWORDS: Dict[str, str] = {
    "FranServe"        : r"\bfran\s*serve|franservesupport|franserve\b",
    "IFPG"             : r"\bifpg\b",
    "BAI"              : r"\bb\.?a\.?i\b|business\W*alliance",
    "TES"              : r"\btes\b|the\s*entrepreneur.?s\s*source|franchisesource|esourcecoach",
    "FranNet"          : r"\bfrannet\b",
    "FBA"              : r"\bfba\b|franchiseba|franchiseiba|fbamembers",
    "TPF"              : r"\btpf\b|the\s*perfect\s*franchise",
    "FCC"              : r"\bfcc\b|franchise\s*consulting\s*company",
    "Franchise Empire" : r"\bfranchise\s*empire\b",
    "SFA Advisors"     : r"\bsfa\s*advisors\b|\bsuccess\s*franchise\s*advisors\b|\bsuccess\s*fran\b",
}
gv_BROKER_DOMAIN_MAP: Dict[str, str] = {
    "franserve.com"                 : "FranServe",
    "franservesupport.com"          : "FranServe",
    "focusonfranchising.com"        : "FranServe",
    "ifpg.org"                      : "IFPG",
    "ifpg.com"                      : "IFPG",
    "businessallianceinc.com"       : "BAI",
    "zizefranchise.com"             : "BAI",
    "inspirefranchiseconsulting.com": "BAI",
    "franchise-connector.com"       : "BAI",
    "franchiseconnector.com"        : "BAI",
    "markfranchise.com"             : "BAI",
    "franchisesource.com"           : "TES",
    "esourcecoach.com"              : "TES",
    "frannet.com"                   : "FranNet",
    "franchiseba.com"               : "FBA",
    "franchiseiba.com"              : "FBA",
    "fbamembers.com"                : "FBA",
    "theperfectfranchise.com"       : "TPF",
    "thefranchiseconsultingcompany.com": "FCC",
    "franchiseempire.com"           : "Franchise Empire",
    "sfaadvisors.com"               : "SFA Advisors",
    "successfran.net"               : "SFA Advisors",
    "securefranchise.com"           : "SFA Advisors",
    "successfranchiseadvisors.com"  : "SFA Advisors",
}
gv_BROKER_ORDER = [
    "IFPG",
    "FranServe",
    "BAI",
    "TES",
    "FranNet",
    "FBA",
    "TPF",
    "FCC",
    "Franchise Empire",
    "SFA Advisors",
    "Others",
]

# ============================== Date/Time Window ==============================
def last_completed_week_utc(
    lv_now_utc: Optional[datetime] = None,
) -> Tuple[datetime, datetime, datetime, str, str]:
    """
    Last fully completed week (UTC):
      start (inclusive)  = previous Sun 00:00:00
      end_exclusive      = this    Sun 00:00:00
      end_inclusive      = this    Sat 23:59:59 (labels only)
    """
    lv_now = lv_now_utc or datetime.now(timezone.utc).replace(microsecond=0)
    lv_days_since_sun = (lv_now.weekday() + 1) % 7
    lv_this_sun = (
        lv_now
        - timedelta(days=lv_days_since_sun)
    ).replace(hour=0, minute=0, second=0, microsecond=0)
    lv_prior_sun = lv_this_sun - timedelta(days=7)

    lv_start = lv_prior_sun
    lv_end_exclusive = lv_this_sun
    lv_end_inclusive = lv_this_sun - timedelta(seconds=1)

    lv_week_label = f"{lv_start:%m.%d}-{lv_end_inclusive:%m.%d.%y}"
    lv_file_label = f"{lv_end_inclusive:%Y-%m-%d}"
    return lv_start, lv_end_inclusive, lv_end_exclusive, lv_week_label, lv_file_label

# ================================ Graph Client ================================
class GraphClient:
    authority = "https://login.microsoftonline.com/{tenant}"
    scope     = ["https://graph.microsoft.com/.default"]
    base      = "https://graph.microsoft.com/v1.0"

    def __init__(self, lv_tenant_id: str, lv_client_id: str,
                 lv_client_secret: str, lv_mailbox: str):
        self.tenant_id      = lv_tenant_id
        self.client_id      = lv_client_id
        self.client_secret  = lv_client_secret
        self.mailbox        = lv_mailbox
        self._token: Optional[str] = None

    def authenticate(self) -> str:
        lv_app = msal.ConfidentialClientApplication(
            self.client_id,
            authority=self.authority.format(tenant=self.tenant_id),
            client_credential=self.client_secret,
        )
        lv_result = lv_app.acquire_token_for_client(scopes=self.scope)
        if "access_token" not in lv_result:
            raise SystemExit("Graph token error:\n" + json.dumps(lv_result, indent=2))
        self._token = lv_result["access_token"]
        return self._token

    def _headers(self) -> dict:
        if not self._token:
            self.authenticate()
        return {
            "Authorization": f"Bearer {self._token}",
            "User-Agent": gv_USER_AGENT,
        }

    def _get(self, lv_url: str, params: dict | None = None) -> requests.Response:
        lv_headers = self._headers()
        lv_backoff = 2
        for lv_attempt in range(1, gv_MAX_RETRIES + 1):
            try:
                lv_resp = requests.get(
                    lv_url,
                    headers=lv_headers,
                    params=params,
                    timeout=gv_HTTP_TIMEOUT,
                )
            except requests.RequestException as lv_err:
                gv_LOG.warning(
                    "Network error (%d/%d): %s",
                    lv_attempt,
                    gv_MAX_RETRIES,
                    lv_err,
                )
                time.sleep(lv_backoff)
                lv_backoff = min(lv_backoff * 2, 32)
                continue

            if lv_resp.status_code in (429, 500, 502, 503, 504):
                lv_wait = int(lv_resp.headers.get("Retry-After", lv_backoff))
                gv_LOG.warning(
                    "Graph %s — waiting %ss (%d/%d)",
                    lv_resp.status_code,
                    lv_wait,
                    lv_attempt,
                    gv_MAX_RETRIES,
                )
                time.sleep(lv_wait)
                lv_backoff = min(lv_backoff * 2, 32)
                continue

            if lv_resp.status_code == 401:
                gv_LOG.warning("401 Unauthorized — refreshing token")
                self.authenticate()
                lv_headers = self._headers()
                continue

            if lv_resp.status_code == 403:
                raise SystemExit("403 Forbidden. Check App Access Policy.")

            lv_resp.raise_for_status()
            return lv_resp

        # if we exhausted retries
        lv_resp.raise_for_status()
        return lv_resp

    def _list_child_folders(self, lv_parent_id: str | None = None) -> List[dict]:
        lv_url = (
            f"{self.base}/users/{self.mailbox}/mailFolders"
            + (f"/{lv_parent_id}/childFolders" if lv_parent_id else "")
        )
        lv_params = {"$top": 50, "$select": "id,displayName,childFolderCount"}
        lv_out: List[dict] = []

        while True:
            lv_data = self._get(lv_url, params=lv_params).json()
            lv_out.extend(lv_data.get("value", []))
            lv_next = lv_data.get("@odata.nextLink")
            if not lv_next:
                break
            lv_url, lv_params = lv_next, None

        lv_final: List[dict] = []
        lv_seen: set[str] = set()
        for lv_folder in lv_out:
            if lv_folder["id"] not in lv_seen:
                lv_final.append(lv_folder)
                lv_seen.add(lv_folder["id"])
            if lv_folder.get("childFolderCount", 0) > 0:
                lv_final.extend(self._list_child_folders(lv_folder["id"]))
        return lv_final

    def list_all_folders(self) -> List[dict]:
        lv_folders = self._list_child_folders(None)
        gv_LOG.info("Discovered %d mail folders.", len(lv_folders))
        return lv_folders

    def fetch_messages_in_folder(
        self,
        lv_folder_id: str,
        lv_start_iso: str,
        lv_end_iso: str,
    ) -> Iterable[dict]:
        lv_url = f"{self.base}/users/{self.mailbox}/mailFolders/{lv_folder_id}/messages"
        lv_params = {
            "$filter": f"receivedDateTime ge {lv_start_iso} and receivedDateTime lt {lv_end_iso}",
            "$orderby": "receivedDateTime asc",
            "$top": 50,
            "$select": (
                "id,internetMessageId,conversationId,receivedDateTime,subject,"
                "from,toRecipients,ccRecipients,bccRecipients,bodyPreview,internetMessageHeaders"
            ),
        }

        while True:
            lv_data = self._get(lv_url, params=lv_params).json()
            for lv_msg in lv_data.get("value", []):
                yield lv_msg
            lv_next = lv_data.get("@odata.nextLink")
            if not lv_next:
                break
            lv_url, lv_params = lv_next, None

    def fetch_full_body(self, lv_message_id: str) -> str:
        lv_url = f"{self.base}/users/{self.mailbox}/messages/{lv_message_id}"
        lv_params = {"$select": "body"}
        lv_data = self._get(lv_url, params=lv_params).json()
        lv_content = (((lv_data.get("body") or {}).get("content")) or "")
        return lv_content

# ============================ Classification Helpers ==========================
gv_RE_PREFIX = re.compile(r"^\s*re\s*[:\-]\s*", re.IGNORECASE)
gv_FW_PREFIX = re.compile(r"^\s*(fw|fwd)\s*[:\-]\s*", re.IGNORECASE)
gv_RE_REPLY_IN_CHAT = re.compile(r"reply in chat", re.IGNORECASE)

def is_reply(lv_s: str) -> bool:
    return bool(gv_RE_PREFIX.match(lv_s or ""))

def is_forward(lv_s: str) -> bool:
    return bool(gv_FW_PREFIX.match(lv_s or ""))

def is_reply_in_chat(lv_subject: str, lv_body_preview: str) -> bool:
    """
    Treat 'Reply in Chat ...' notifications as follow-up emails.
    We check both Subject and BodyPreview for safety.
    """
    lv_combined = f"{lv_subject or ''} {lv_body_preview or ''}"
    return bool(gv_RE_REPLY_IN_CHAT.search(lv_combined))

def _norm(lv_text: str) -> str:
    lv_s = unicodedata.normalize("NFKC", lv_text or "").lower()
    lv_s = re.sub(r"[\u00A0\u2000-\u200B\u2060]", " ", lv_s)
    lv_s = re.sub(r"\s+", " ", lv_s).strip()
    return lv_s

def _html_to_text(lv_html_str: str) -> str:
    if not lv_html_str:
        return ""
    lv_txt = re.sub(r"(?is)<(script|style).*?>.*?</\1>", " ", lv_html_str)
    lv_txt = re.sub(r"(?is)<br\s*/?>", "\n", lv_txt)
    lv_txt = re.sub(r"(?is)</p\s*>", "\n", lv_txt)
    lv_txt = re.sub(r"(?is)<.*?>", " ", lv_txt)
    lv_txt = html.unescape(lv_txt)
    lv_txt = re.sub(r"\r", "\n", lv_txt)
    return re.sub(r"[ \t]+", " ", lv_txt)

def _addr_list(*lv_lists: List[dict] | None) -> List[str]:
    lv_addrs: List[str] = []
    for lv_coll in lv_lists:
        for lv_item in (lv_coll or []):
            lv_addr = (lv_item.get("emailAddress") or {}).get("address", "")
            if lv_addr:
                lv_addrs.append(lv_addr.lower())
    return lv_addrs

def _match_brand_in_text(lv_haystack: str) -> Optional[str]:
    lv_hay = lv_haystack.lower()
    for lv_brand, lv_needles in gv_BRAND_ADDRESSES.items():
        for lv_n in lv_needles:
            lv_tok = lv_n.lower()
            if "@" in lv_tok:
                if re.search(r"\b" + re.escape(lv_tok) + r"\b", lv_hay):
                    return lv_brand
            else:
                if re.search(r"(?<![a-z0-9])" + re.escape(lv_tok) + r"\b", lv_hay):
                    return lv_brand
    return None

def brand_from_recipients(
    lv_to_list: List[dict] | None,
    lv_cc_list: List[dict] | None,
    lv_bcc_list: List[dict] | None,
) -> tuple[Optional[str], str]:
    lv_addrs = _addr_list(lv_to_list, lv_cc_list, lv_bcc_list)
    lv_hay = " ".join(lv_addrs)
    lv_brand = _match_brand_in_text(lv_hay)
    return lv_brand, ("To/Cc/Bcc" if lv_brand else "")

gv_ROUTING_HEADERS = {
    "to",
    "cc",
    "bcc",
    "delivered-to",
    "x-original-to",
    "x-envelope-to",
    "x-forwarded-to",
    "return-path",
}

def brand_from_headers(lv_headers: List[dict] | None) -> tuple[Optional[str], str]:
    if not lv_headers:
        return (None, "")
    lv_vals: List[str] = []
    for lv_h in lv_headers:
        lv_name = (lv_h.get("name") or "").lower()
        if lv_name in gv_ROUTING_HEADERS:
            lv_vals.append(lv_h.get("value", ""))
    if not lv_vals:
        return (None, "")
    lv_joined = " ".join(lv_vals)
    lv_brand = _match_brand_in_text(lv_joined)
    return lv_brand, (
        "Headers(" + ",".join(sorted(gv_ROUTING_HEADERS)) + ")" if lv_brand else ""
    )

def broker_from_subject(lv_subject: str) -> Optional[str]:
    lv_s = _norm(lv_subject)
    for lv_broker, lv_pattern in gv_BROKER_SUBJECT_KEYWORDS.items():
        if re.search(lv_pattern, lv_s):
            return lv_broker
    return None

def broker_from_sender(lv_from_obj: dict | None) -> str:
    lv_addr = ((lv_from_obj or {}).get("emailAddress") or {}).get("address", "") or ""
    lv_dom = lv_addr.split("@")[-1].lower() if "@" in lv_addr else lv_addr.lower()
    for lv_d, lv_canonical in gv_BROKER_DOMAIN_MAP.items():
        if lv_d in lv_dom:
            return lv_canonical
    return "Others"

gv_RE_FWD_FROM_ANGLE = re.compile(
    r"^from:\s*[^<\r\n]*<\s*([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})\s*>",
    re.IGNORECASE | re.MULTILINE,
)
gv_RE_FWD_FROM_BARE = re.compile(
    r"^from:\s*([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})",
    re.IGNORECASE | re.MULTILINE,
)

def forwarded_broker_from_bodypreview(lv_body_preview: str) -> Optional[str]:
    if not lv_body_preview:
        return None
    lv_m = (
        gv_RE_FWD_FROM_ANGLE.search(lv_body_preview)
        or gv_RE_FWD_FROM_BARE.search(lv_body_preview)
    )
    if lv_m:
        lv_email = lv_m.group(1).strip().lower()
        lv_dom = lv_email.split("@")[-1]
        for lv_d, lv_canonical in gv_BROKER_DOMAIN_MAP.items():
            if lv_d in lv_dom:
                return lv_canonical

    for lv_broker, lv_pattern in gv_BROKER_SUBJECT_KEYWORDS.items():
        if re.search(lv_pattern, (lv_body_preview or "").lower()):
            return lv_broker
    return None

# ---------- Territory matchers ----------
gv_TERR_PATTERNS = (
    re.compile(r"\bin\s+([a-z .'\-]+,\s*[a-z]{2})\b", re.IGNORECASE),
    re.compile(r"\b([a-z .'\-]+,\s*[a-z]{2})\b", re.IGNORECASE),
)
gv_BODY_TERR_PATTERNS = (
    re.compile(r"desired\s*territor(?:y|ies)\s*:\s*([^\r\n]+)", re.IGNORECASE),
    re.compile(r"territor(?:y|ies)\s*requested\s*:\s*([^\r\n]+)", re.IGNORECASE),
    re.compile(r"territor(?:y|ies)\s*:\s*([^\r\n]+)", re.IGNORECASE),
    re.compile(r"location\s*:\s*([^\r\n]+)", re.IGNORECASE),
)
gv_ANYWHERE_TERR_FALLBACK = re.compile(
    r"territor(?:y|ies)[^A-Za-z]{0,20}([A-Za-z .'\-]+,\s*[A-Za-z]{2})",
    re.IGNORECASE | re.DOTALL,
)

def territory_from_subject(lv_subject: str) -> str:
    lv_s = _norm(lv_subject)
    for lv_pat in gv_TERR_PATTERNS:
        lv_m = lv_pat.search(lv_s)
        if lv_m:
            return lv_m.group(1).title()
    return ""

def _clean_territory(lv_raw: str) -> str:
    lv_s = _norm(lv_raw or "")
    lv_s = re.split(r"[|/;\n\r]", lv_s)[0].strip()
    lv_s = re.sub(r"\s+", " ", lv_s)
    return lv_s.title()

def territory_from_any(
    lv_subject: str,
    lv_body_preview: str,
    lv_full_body_text: str | None = None,
) -> str:
    lv_terr = territory_from_subject(lv_subject)
    if lv_terr:
        return lv_terr

    for lv_pat in gv_BODY_TERR_PATTERNS:
        lv_m = lv_pat.search(lv_body_preview or "")
        if lv_m:
            return _clean_territory(lv_m.group(1))

    lv_m = gv_ANYWHERE_TERR_FALLBACK.search(lv_body_preview or "")
    if lv_m:
        return _clean_territory(lv_m.group(1))

    if lv_full_body_text:
        for lv_pat in gv_BODY_TERR_PATTERNS:
            lv_m = lv_pat.search(lv_full_body_text)
            if lv_m:
                return _clean_territory(lv_m.group(1))
        lv_m = gv_ANYWHERE_TERR_FALLBACK.search(lv_full_body_text)
        if lv_m:
            return _clean_territory(lv_m.group(1))
        lv_m = re.search(
            r"\b([A-Za-z .'\-]+,\s*[A-Za-z]{2})\b",
            lv_full_body_text,
        )
        if lv_m:
            return _clean_territory(lv_m.group(1))

    return ""

def pretty_label_from_domain(lv_email_or_domain: str) -> str:
    lv_dom = (lv_email_or_domain or "").split("@")[-1].lower()
    if not lv_dom or "." not in lv_dom:
        return "Unknown"
    lv_core = lv_dom.split(".")[0]
    lv_chunks = re.split(r"[\W_]+", lv_core)
    if not lv_chunks or lv_chunks == [""]:
        return lv_core.title()
    return " ".join(
        [
            lv_c.upper() if len(lv_c) <= 3 else lv_c.title()
            for lv_c in lv_chunks
            if lv_c
        ]
    )

# ================================ Excel Helpers ===============================
def ensure_out_dir() -> None:
    os.makedirs(gv_OUT_DIR, exist_ok=True)

def unique_week_filepath(lv_end_label: str) -> str:
    lv_base = os.path.join(gv_OUT_DIR, f"Territory_Checks_{lv_end_label}.xlsx")
    if not os.path.exists(lv_base):
        return lv_base
    lv_i = 1
    while True:
        lv_cand = os.path.join(
            gv_OUT_DIR,
            f"Territory_Checks_{lv_end_label} ({lv_i}).xlsx",
        )
        if not os.path.exists(lv_cand):
            return lv_cand
        lv_i += 1

def try_open_workbook(lv_path: str) -> Optional[Workbook]:
    if not os.path.exists(lv_path):
        lv_wb = Workbook()
        lv_wb.active.title = "Index"
        return lv_wb
    try:
        return load_workbook(lv_path)
    except PermissionError:
        return None

# ================================== Main ======================================
def run() -> dict:
    # ---- Env validation
    lv_missing = [
        lv_k
        for lv_k, lv_v in {
            "GRAPH_TENANT_ID"    : gv_TENANT_ID,
            "GRAPH_CLIENT_ID"    : gv_CLIENT_ID,
            "GRAPH_CLIENT_SECRET": gv_CLIENT_SECRET,
            "MASTER_MAILBOX"     : gv_MASTER_MAILBOX,
            "OUT_DIR"            : gv_OUT_DIR,
        }.items()
        if not lv_v
    ]
    if lv_missing:
        raise SystemExit("Missing environment variables: " + ", ".join(lv_missing))

    ensure_out_dir()
    lv_start_dt, lv_end_dt, lv_end_excl, lv_week_label, lv_file_label = (
        last_completed_week_utc()
    )
    lv_run_ts = datetime.now(timezone.utc).replace(microsecond=0)

    gv_LOG.info(
        "Processing last completed week: %s → %s (%s)",
        lv_start_dt,
        lv_end_dt,
        lv_week_label,
    )

    lv_client = GraphClient(
        gv_TENANT_ID,
        gv_CLIENT_ID,
        gv_CLIENT_SECRET,
        gv_MASTER_MAILBOX,
    )
    lv_client.authenticate()

    lv_start_iso = lv_start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    lv_end_iso   = lv_end_excl.strftime("%Y-%m-%dT%H:%M:%SZ")

    lv_details_rows: List[dict] = []
    lv_audit_rows:   List[dict] = []

    # ---- Scan ALL folders
    for lv_folder in lv_client.list_all_folders():
        lv_fid   = lv_folder["id"]
        lv_fname = lv_folder.get("displayName", "")
        gv_LOG.info("Scanning folder: %s", lv_fname)

        for lv_msg in lv_client.fetch_messages_in_folder(
            lv_fid,
            lv_start_iso,
            lv_end_iso,
        ):
            lv_subj = (lv_msg.get("subject") or "").strip()

            # Basic markers
            lv_is_reply   = is_reply(lv_subj)
            lv_is_forward = is_forward(lv_subj)
            lv_body_preview = (lv_msg.get("bodyPreview") or "").strip()

            # --- Decide skip reason (but DO NOT drop from Audit)
            lv_skipped_reason = ""

            # 1) Explicit "Reply in Chat" follow-ups from brokers
            if is_reply_in_chat(lv_subj, lv_body_preview):
                lv_skipped_reason = "Reply in Chat"
            # 2) Normal replies (Re:) if configured to skip
            elif gv_SKIP_REPLIES and lv_is_reply:
                lv_skipped_reason = "Reply"
            # 3) Forwards if counting is turned off
            elif (not gv_COUNT_FORWARDS) and lv_is_forward:
                lv_skipped_reason = "Forward (disabled)"

            # ----- Brand: To/Cc/Bcc → headers → Unmapped
            lv_b_to_cc_bcc, lv_source1 = brand_from_recipients(
                lv_msg.get("toRecipients"),
                lv_msg.get("ccRecipients"),
                lv_msg.get("bccRecipients"),
            )
            lv_b_hdrs: Optional[str]
            lv_source2: str
            lv_b_hdrs, lv_source2 = (None, "")
            if not lv_b_to_cc_bcc:
                lv_b_hdrs, lv_source2 = brand_from_headers(
                    lv_msg.get("internetMessageHeaders")
                )

            lv_brand        = lv_b_to_cc_bcc or lv_b_hdrs or "Unmapped"
            lv_brand_source = lv_source1 or lv_source2 or "Unmapped"

            # ----- Sender / preview
            lv_from_obj    = (lv_msg.get("from") or {}).get("emailAddress") or {}
            lv_sender      = (lv_from_obj.get("address") or "").strip()
            lv_sender_name = (lv_from_obj.get("name") or "").strip() or lv_sender

            # ----- Broker: subject → forwarded header → domain
            lv_broker_subj = broker_from_subject(lv_subj)
            lv_broker_fw   = None if lv_broker_subj else \
                forwarded_broker_from_bodypreview(lv_body_preview)
            lv_broker_dom  = broker_from_sender(lv_msg.get("from"))
            lv_chosen_broker = lv_broker_subj or lv_broker_fw or lv_broker_dom

            # If still Others, produce friendly label in details
            lv_detail_broker_label = lv_chosen_broker
            lv_others_label: Optional[str] = None
            if lv_chosen_broker == "Others":
                lv_pretty = pretty_label_from_domain(lv_sender)
                lv_detail_broker_label = f"Others | {lv_pretty}"
                lv_others_label = lv_pretty

            # ----- Territory anywhere: subject → bodyPreview → full body (lazy)
            lv_terr      = territory_from_any(lv_subj, lv_body_preview, None)
            lv_full_text = None
            if not lv_terr:
                try:
                    lv_body_html = lv_client.fetch_full_body(lv_msg["id"])
                    lv_full_text = _html_to_text(lv_body_html)
                    lv_terr = territory_from_any(
                        lv_subj,
                        lv_body_preview,
                        lv_full_text,
                    )
                except Exception as lv_e:
                    gv_LOG.debug(
                        "Full body fetch failed for %s: %s",
                        lv_msg.get("id"),
                        lv_e,
                    )

            lv_recv    = lv_msg.get("receivedDateTime", "")
            lv_recv_dt = lv_recv[:10] if lv_recv else ""

            # ----- Build Audit row (ALL emails, even skipped)
            lv_to_str = ";".join(
                [
                    (lv_x.get("emailAddress") or {}).get("address", "")
                    for lv_x in (lv_msg.get("toRecipients") or [])
                ]
            )
            lv_cc_str = ";".join(
                [
                    (lv_x.get("emailAddress") or {}).get("address", "")
                    for lv_x in (lv_msg.get("ccRecipients") or [])
                ]
            )
            lv_bcc_str = ";".join(
                [
                    (lv_x.get("emailAddress") or {}).get("address", "")
                    for lv_x in (lv_msg.get("bccRecipients") or [])
                ]
            )

            lv_audit_rows.append({
                "Folder"                : lv_fname,
                "Brand"                 : lv_brand,
                "Brand Source"          : lv_brand_source,
                "IsForward"             : lv_is_forward,
                "IsReply"               : lv_is_reply,
                "Subject"               : lv_subj,
                "From"                  : lv_sender,
                "To"                    : lv_to_str,
                "CC"                    : lv_cc_str,
                "BCC"                   : lv_bcc_str,
                "BodyPreview"           : lv_body_preview,
                "ReceivedUTC"           : lv_recv,
                "FetchedFullBody"       : bool(lv_full_text),
                "Chosen Broker (bucket)": lv_chosen_broker,
                "SkippedReason"         : lv_skipped_reason,
            })

            # ----- Only add to main details if NOT skipped
            if lv_skipped_reason:
                continue

            lv_details_rows.append({
                "Brand"              : lv_brand,
                "Broker Brand"       : lv_detail_broker_label,
                "Summary Bucket"     : lv_chosen_broker,
                "Others Name"        : lv_others_label or "",
                "Broker Name"        : lv_sender_name,
                "Territory"          : lv_terr,
                "Received (UTC Date)": lv_recv_dt,
                "Subject"            : lv_subj,
                "From"               : lv_sender,
                "Folder"             : lv_fname,
                "Brand Source"       : lv_brand_source,
                "ConversationId"     : lv_msg.get("conversationId", ""),
                "InternetMessageId"  : lv_msg.get("internetMessageId", ""),
            })

    # ---- Build DataFrame & light duplicate guard
    lv_df = pd.DataFrame(lv_details_rows)
    if lv_df.empty:
        gv_LOG.info(
            "No messages in the window; writing an empty workbook with all brand sheets."
        )
        empty_df = pd.DataFrame(
            columns=[
                "Brand",
                "Broker Brand",
                "Summary Bucket",
                "Others Name",
                "Broker Name",
                "Territory",
                "Received (UTC Date)",
                "Subject",
                "From",
                "Folder",
                "Brand Source",
                "ConversationId",
                "InternetMessageId",
            ]
        )
        excel_local_path = _write_workbook(
            empty_df,
            lv_audit_rows,
            lv_week_label,
            lv_file_label,
        )

        excel_gcs_uri = upload_file_to_gcs(
            excel_local_path,
            gv_GCS_PREFIX_EXCEL,
        )

        return {
            "excel_local_path": excel_local_path,
            "excel_gcs_uri"  : excel_gcs_uri,
            "raw_local_path" : None,
            "raw_gcs_uri"    : None,
        }

    if "InternetMessageId" in lv_df.columns:
        lv_df = lv_df.sort_values(
            by=["InternetMessageId", "Received (UTC Date)"]
        )
        lv_df = lv_df.drop_duplicates(
            subset=["InternetMessageId"],
            keep="first",
        )

    # ---- De-duplicate by Brand+Broker+Territory (when territory known)
    def _canon_terr(lv_x: str) -> str:
        lv_s = _norm(lv_x or "")
        lv_s = re.sub(r"[^a-z0-9 ,.'\-]", " ", lv_s)
        lv_s = re.sub(r"\s+", " ", lv_s).strip()
        return lv_s

    lv_df["__TerrKey__"] = lv_df["Territory"].map(_canon_terr)
    lv_with_terr    = lv_df[lv_df["__TerrKey__"] != ""]
    lv_without_terr = lv_df[lv_df["__TerrKey__"] == ""]
    lv_deduped = lv_with_terr.drop_duplicates(
        subset=["Brand", "Summary Bucket", "Broker Brand", "__TerrKey__"],
        keep="first",
    )
    lv_df = pd.concat(
        [lv_deduped, lv_without_terr],
        ignore_index=True,
    )

    # ---- Sort & sequence
    lv_df["Received (UTC Date)"] = pd.to_datetime(
        lv_df["Received (UTC Date)"],
        errors="coerce",
    )
    lv_df.sort_values(
        by=["Brand", "Summary Bucket", "Received (UTC Date)", "Subject"],
        inplace=True,
    )
    lv_df["Seq"] = (
        lv_df.groupby(["Brand", "Broker Brand"])
        .cumcount()
        .add(1)
    )

    # ==================== BigQuery bronze DataFrames ====================
    # 1) territory_checks_raw  (non-skipped, de-duplicated rows used for counts)
    df_territory = lv_df.copy()
    df_territory["run_date_from"] = lv_start_dt.date()
    df_territory["run_date_to"]   = lv_end_dt.date()
    df_territory["run_timestamp"] = lv_run_ts

    df_territory = df_territory.rename(
        columns={
            "Brand"              : "brand_name",
            "Broker Brand"       : "broker_brand",
            "Broker Name"        : "broker_name",
            "Territory"          : "territory",
            "Received (UTC Date)": "received_utc",
            "Seq"                : "seq",
            "Subject"            : "subject",
        }
    )

    df_territory = df_territory[
        [
            "brand_name",
            "broker_brand",
            "broker_name",
            "territory",
            "received_utc",
            "seq",
            "subject",
            "run_date_from",
            "run_date_to",
            "run_timestamp",
        ]
    ]

    # 2) unmapped_territory_checks  (only rows where brand was Unmapped)
    df_unmapped = lv_df[lv_df["Brand"] == "Unmapped"].copy()
    df_unmapped["run_date_from"] = lv_start_dt.date()
    df_unmapped["run_date_to"]   = lv_end_dt.date()
    df_unmapped["run_timestamp"] = lv_run_ts

    df_unmapped = df_unmapped.rename(
        columns={
            "Brand"              : "brand_name",
            "Brand Source"       : "brand_source",
            "Broker Brand"       : "broker_brand",
            "Broker Name"        : "broker_name",
            "Territory"          : "territory",
            "Received (UTC Date)": "received_utc",
            "Subject"            : "subject",
            "From"               : "from_email",
            "Folder"             : "folder_name",
        }
    )

    df_unmapped = df_unmapped[
        [
            "brand_name",
            "brand_source",
            "broker_brand",
            "broker_name",
            "territory",
            "received_utc",
            "subject",
            "from_email",
            "folder_name",
            "run_date_from",
            "run_date_to",
            "run_timestamp",
        ]
    ]

    # 3) audit_territory_checks  (ALL emails, including skipped)
    df_audit = pd.DataFrame(lv_audit_rows)
    if not df_audit.empty:
        df_audit["run_date_from"] = lv_start_dt.date()
        df_audit["run_date_to"]   = lv_end_dt.date()
        df_audit["run_timestamp"] = lv_run_ts

        df_audit = df_audit.rename(
            columns={
                "Folder"                : "folder_name",
                "Brand"                 : "brand_name",
                "Brand Source"          : "brand_source",
                "IsForward"             : "is_forward",
                "IsReply"               : "is_reply",
                "Subject"               : "subject",
                "From"                  : "from_email",
                "To"                    : "to_email",
                "CC"                    : "cc_email",
                "BCC"                   : "bcc_email",
                "BodyPreview"           : "body_preview",
                "ReceivedUTC"           : "received_utc",
                "FetchedFullBody"       : "fetched_full_body",
                "Chosen Broker (bucket)": "chosen_broker",
                # SkippedReason stays in Excel only
            }
        )

        df_audit = df_audit[
            [
                "folder_name",
                "brand_name",
                "brand_source",
                "is_forward",
                "is_reply",
                "subject",
                "from_email",
                "to_email",
                "cc_email",
                "bcc_email",
                "body_preview",
                "received_utc",
                "fetched_full_body",
                "chosen_broker",
                "run_date_from",
                "run_date_to",
                "run_timestamp",
            ]
        ]

    # ==================== Save CSVs for GCS / BigQuery ====================

    raw_filename      = f"territory_checks_raw_{lv_file_label}.csv"
    raw_unmapped_file = f"territory_unmapped_{lv_file_label}.csv"
    audit_filename    = f"territory_audit_{lv_file_label}.csv"

    raw_local_path      = os.path.join(gv_OUT_DIR, raw_filename)
    unmapped_local_path = os.path.join(gv_OUT_DIR, raw_unmapped_file)
    audit_local_path    = os.path.join(gv_OUT_DIR, audit_filename)

    # ---- Normalize received_utc to full timestamps for BigQuery ----
    common_date_format = "%Y-%m-%d %H:%M:%S"

    for df in (df_territory, df_unmapped, df_audit):
        if df is not None and not df.empty and "received_utc" in df.columns:
            df["received_utc"] = pd.to_datetime(
                df["received_utc"],
                errors="coerce",
            )

    # ---- Territory + Unmapped: normal CSV with date_format ----
    df_territory.to_csv(
        raw_local_path,
        index=False,
        date_format=common_date_format,
    )

    df_unmapped.to_csv(
        unmapped_local_path,
        index=False,
        date_format=common_date_format,
    )

    # ---- Audit: special handling (quotes + clean run_timestamp) ----
    if not df_audit.empty:
        if "run_timestamp" in df_audit.columns:
            df_audit["run_timestamp"] = (
                pd.to_datetime(df_audit["run_timestamp"], errors="coerce")
                  .dt.strftime(common_date_format)
            )

        df_audit.to_csv(
            audit_local_path,
            index=False,
            quoting=csv.QUOTE_ALL,   # important: handles commas/newlines
            lineterminator="\n",
        )

    gv_LOG.info(
        "Saved CSVs for BigQuery: %s, %s, %s",
        raw_local_path,
        unmapped_local_path,
        audit_local_path,
    )

    # ---- Write Excel workbook for business users (uses lv_df + lv_audit_rows)
    excel_local_path = _write_workbook(
        lv_df,
        lv_audit_rows,
        lv_week_label,
        lv_file_label,
    )

    # ---- Upload all files to GCS (if configured)
    raw_gcs_uri      = upload_file_to_gcs(raw_local_path,      gv_GCS_PREFIX_RAW + "/territory")
    unmapped_gcs_uri = upload_file_to_gcs(unmapped_local_path, gv_GCS_PREFIX_RAW + "/unmapped")
    audit_gcs_uri    = upload_file_to_gcs(audit_local_path,    gv_GCS_PREFIX_RAW + "/audit")
    excel_gcs_uri    = upload_file_to_gcs(excel_local_path,    gv_GCS_PREFIX_EXCEL)

    # ---- Load into BigQuery bronze (if configured)
    if raw_gcs_uri:
        load_csv_to_bigquery(
            raw_gcs_uri,
            gv_BQ_DATASET_BRONZE,
            gv_BQ_TABLE_TERRITORY,
        )
    if unmapped_gcs_uri:
        load_csv_to_bigquery(
            unmapped_gcs_uri,
            gv_BQ_DATASET_BRONZE,
            gv_BQ_TABLE_UNMAPPED,
        )
    if audit_gcs_uri:
        load_csv_to_bigquery(
            audit_gcs_uri,
            gv_BQ_DATASET_BRONZE,
            gv_BQ_TABLE_AUDIT,
        )

    return {
        "excel_local_path": excel_local_path,
        "excel_gcs_uri"   : excel_gcs_uri,
        "raw_local_path"  : raw_local_path,
        "raw_gcs_uri"     : raw_gcs_uri,
    }

# ------------------------------- Writer ---------------------------------------
def _write_workbook(
    lv_df: pd.DataFrame,
    lv_audit_rows: List[dict],
    lv_week_label: str,
    lv_file_label: str,
) -> str:
    lv_seen_brokers = [
        lv_b
        for lv_b in lv_df.get(
            "Summary Bucket",
            pd.Series([], dtype=str),
        ).unique().tolist()
        if lv_b not in gv_BROKER_ORDER and lv_b not in (None, "Unmapped")
    ]
    lv_broker_order_full = gv_BROKER_ORDER + sorted(
        [lv_b for lv_b in lv_seen_brokers if isinstance(lv_b, str)]
    )

    lv_target_path = unique_week_filepath(lv_file_label)
    lv_wb = try_open_workbook(lv_target_path)
    if lv_wb is None:
        lv_i = 1
        while lv_wb is None:
            lv_alt = os.path.join(
                gv_OUT_DIR,
                f"Territory_Checks_{lv_file_label} ({lv_i}).xlsx",
            )
            lv_wb = try_open_workbook(lv_alt)
            if lv_wb:
                lv_target_path = lv_alt
                break
            lv_i += 1

    # ---- Per-brand data sheets
    if not lv_df.empty:
        for lv_brand, lv_sub in lv_df.groupby("Brand"):
            lv_sheet_name = lv_brand
            if lv_sheet_name not in lv_wb.sheetnames:
                lv_wb.create_sheet(lv_sheet_name)
            lv_ws = lv_wb[lv_sheet_name]
            lv_ws.delete_rows(1, lv_ws.max_row)

            lv_cols_detail = [
                "Broker Brand",
                "Broker Name",
                "Territory",
                "Received (UTC Date)",
                "Seq",
            ]
            if gv_SHOW_SUBJECT_IN_DETAILS:
                lv_cols_detail.append("Subject")

            lv_sub_out = lv_sub[lv_cols_detail + ["Summary Bucket"]].copy()
            lv_sub_out["__order__"] = lv_sub_out["Summary Bucket"].apply(
                lambda lv_b: lv_broker_order_full.index(lv_b)
                if lv_b in lv_broker_order_full
                else 999
            )
            lv_sub_out.sort_values(
                by=["__order__", "Received (UTC Date)", "Seq"],
                inplace=True,
            )
            lv_sub_out.drop(columns=["__order__", "Summary Bucket"], inplace=True)

            lv_ws.append(lv_cols_detail)
            for _, lv_r in lv_sub_out.iterrows():
                lv_ws.append([lv_r[lv_c] for lv_c in lv_cols_detail])

            # Summary (start at G)
            lv_col_base = 7
            lv_summary = (
                lv_sub.groupby("Summary Bucket")
                .size()
                .rename("Count")
                .reset_index()
            )
            lv_summary = (
                lv_summary.set_index("Summary Bucket")
                .reindex(lv_broker_order_full, fill_value=0)
                .reset_index()
            )
            lv_total_row = pd.DataFrame(
                [
                    {
                        "Summary Bucket": "Total",
                        "Count": int(lv_summary["Count"].sum()),
                    }
                ]
            )
            lv_summary = pd.concat(
                [lv_summary, lv_total_row],
                ignore_index=True,
            )

            lv_ws.cell(row=1, column=lv_col_base,   value="Broker Brand")
            lv_ws.cell(row=1, column=lv_col_base+1, value=lv_week_label)
            for lv_i, lv_row in lv_summary.iterrows():
                lv_ws.cell(
                    row=lv_i + 2,
                    column=lv_col_base,
                    value=lv_row["Summary Bucket"],
                )
                lv_ws.cell(
                    row=lv_i + 2,
                    column=lv_col_base + 1,
                    value=int(lv_row["Count"]),
                )

            # Others breakdown (below)
            lv_others_sub = lv_sub[lv_sub["Summary Bucket"] == "Others"]
            if not lv_others_sub.empty:
                lv_breakdown = (
                    lv_others_sub.assign(
                        Detail=lv_others_sub["Broker Brand"].str.replace(
                            r"^Others \|\s*",
                            "",
                            regex=True,
                        )
                    )
                    .groupby("Detail")
                    .size()
                    .rename("Count")
                    .reset_index()
                    .sort_values(
                        by=["Count", "Detail"],
                        ascending=[False, True],
                    )
                )
                lv_start_r = len(lv_summary) + 4
                lv_ws.cell(
                    row=lv_start_r - 1,
                    column=lv_col_base,
                    value="Others breakdown",
                )
                lv_ws.cell(
                    row=lv_start_r,
                    column=lv_col_base,
                    value="Detail",
                )
                lv_ws.cell(
                    row=lv_start_r,
                    column=lv_col_base + 1,
                    value="Count",
                )
                for lv_j, lv_row in lv_breakdown.iterrows():
                    lv_ws.cell(
                        row=lv_start_r + lv_j + 1,
                        column=lv_col_base,
                        value=lv_row["Detail"],
                    )
                    lv_ws.cell(
                        row=lv_start_r + lv_j + 1,
                        column=lv_col_base + 1,
                        value=int(lv_row["Count"]),
                    )

    # ---- Ensure every brand has a sheet, even with 0 rows
    lv_cols_detail = [
        "Broker Brand",
        "Broker Name",
        "Territory",
        "Received (UTC Date)",
        "Seq",
    ]
    if gv_SHOW_SUBJECT_IN_DETAILS:
        lv_cols_detail.append("Subject")

    for lv_brand_name in gv_BRAND_ADDRESSES.keys():
        if lv_brand_name not in lv_wb.sheetnames:
            lv_wb.create_sheet(lv_brand_name)
        lv_ws = lv_wb[lv_brand_name]
        if lv_ws.max_row <= 1 and lv_ws.max_column <= 1:
            lv_ws.delete_rows(1, lv_ws.max_row)
            lv_ws.append(lv_cols_detail)
            lv_col_base = 7
            lv_ws.cell(row=1, column=lv_col_base,   value="Broker Brand")
            lv_ws.cell(row=1, column=lv_col_base+1, value=lv_week_label)
            for lv_i, lv_b in enumerate(lv_broker_order_full, start=2):
                lv_ws.cell(row=lv_i, column=lv_col_base,   value=lv_b)
                lv_ws.cell(row=lv_i, column=lv_col_base+1, value=0)
            lv_ws.cell(
                row=len(lv_broker_order_full) + 2,
                column=lv_col_base,
                value="Total",
            )
            lv_ws.cell(
                row=len(lv_broker_order_full) + 2,
                column=lv_col_base + 1,
                value=0,
            )

    # ---- Unmapped
    if "Unmapped" not in lv_wb.sheetnames:
        lv_wb.create_sheet("Unmapped")
    lv_ws_u = lv_wb["Unmapped"]
    lv_ws_u.delete_rows(1, lv_ws_u.max_row)
    lv_cols_u = [
        "Brand",
        "Brand Source",
        "Broker Brand",
        "Broker Name",
        "Territory",
        "Received (UTC Date)",
        "Subject",
        "From",
        "Folder",
    ]
    lv_ws_u.append(lv_cols_u)
    if not lv_df.empty and "Brand" in lv_df.columns:
        for _, lv_r in lv_df[lv_df["Brand"] == "Unmapped"][lv_cols_u].iterrows():
            lv_ws_u.append([lv_r.get(lv_c, "") for lv_c in lv_cols_u])

    # ---- Audit
    if "Audit" not in lv_wb.sheetnames:
        lv_wb.create_sheet("Audit")
    lv_ws_a = lv_wb["Audit"]
    lv_ws_a.delete_rows(1, lv_ws_a.max_row)
    lv_cols_a = [
        "Folder",
        "Brand",
        "Brand Source",
        "IsForward",
        "IsReply",
        "Subject",
        "From",
        "To",
        "CC",
        "BCC",
        "BodyPreview",
        "ReceivedUTC",
        "FetchedFullBody",
        "Chosen Broker (bucket)",
        "SkippedReason",
    ]
    lv_ws_a.append(lv_cols_a)
    for lv_row in lv_audit_rows:
        lv_ws_a.append([lv_row.get(lv_c, "") for lv_c in lv_cols_a])

    # ---- Remove placeholder
    if "Index" in lv_wb.sheetnames and len(lv_wb.sheetnames) > 1:
        del lv_wb["Index"]

    # ---- Save (lock-safe)
    try:
        lv_wb.save(lv_target_path)
        gv_LOG.info("Saved weekly workbook: %s", lv_target_path)
    except PermissionError:
        lv_j = 1
        while True:
            lv_alt2 = os.path.join(
                gv_OUT_DIR,
                f"Territory_Checks_{lv_file_label} (save {lv_j}).xlsx",
            )
            try:
                lv_wb.save(lv_alt2)
                gv_LOG.info("Target locked; saved as: %s", lv_alt2)
                lv_target_path = lv_alt2
                break
            except PermissionError:
                lv_j += 1

    return lv_target_path

def upload_file_to_gcs(local_path: str, prefix: str) -> Optional[str]:
    """
    Upload a local file to GCS.
    Returns the gs:// URI or None if GCS_BUCKET is not configured.
    """
    if not gv_GCS_BUCKET:
        gv_LOG.info("GCS_BUCKET not set; skipping upload for %s", local_path)
        return None

    client = storage.Client()
    bucket = client.bucket(gv_GCS_BUCKET)

    base_name = os.path.basename(local_path)
    prefix = (prefix or "").strip().strip("/")
    blob_name = f"{prefix}/{base_name}" if prefix else base_name

    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

    gcs_uri = f"gs://{gv_GCS_BUCKET}/{blob_name}"
    gv_LOG.info("Uploaded %s to %s", local_path, gcs_uri)
    return gcs_uri

def load_csv_to_bigquery(gcs_uri: str, dataset: str, table: str) -> None:
    """
    Load a CSV at gcs_uri into BigQuery.
    """
    if not gv_BQ_PROJECT:
        gv_LOG.info("BQ_PROJECT not set; skipping BigQuery load for %s", gcs_uri)
        return

    table_id = f"{gv_BQ_PROJECT}.{dataset}.{table}"
    client = bigquery.Client(project=gv_BQ_PROJECT)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,  # schema already created
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # or APPEND later
        allow_quoted_newlines=True,   
        field_delimiter=",",
        encoding="UTF-8",
        max_bad_records=0,
    )

    gv_LOG.info("Starting BigQuery load from %s to %s", gcs_uri, table_id)
    load_job = client.load_table_from_uri(gcs_uri, table_id, job_config=job_config)
    result = load_job.result()
    gv_LOG.info(
        "BigQuery load complete: %d rows loaded to %s",
        result.output_rows,
        table_id,
    )


# ================================= Entrypoint =================================
def main() -> None:
    gv_LOG.info("Starting Territory Checks ETL job...")
    try:
        result = run()
        gv_LOG.info("Job finished.")
        gv_LOG.info("Excel local: %s", result.get("excel_local_path"))
        gv_LOG.info("Excel GCS  : %s", result.get("excel_gcs_uri"))
        gv_LOG.info("Raw local  : %s", result.get("raw_local_path"))
        gv_LOG.info("Raw GCS    : %s", result.get("raw_gcs_uri"))
    except SystemExit as lv_e:
        gv_LOG.error(str(lv_e))
        raise
    except Exception:
        gv_LOG.exception("Unhandled error")
        raise

if __name__ == "__main__":
    main()
