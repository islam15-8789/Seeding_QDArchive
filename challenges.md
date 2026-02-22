# Data Challenges

A comprehensive inventory of data challenges identified in the QDArchive harvester project.

---

## 1. Skipped Data Sources (Licensing / Access Barriers)

These sources cannot be harvested due to restrictive licensing or access requirements.

| Source | Reason | Impact |
|--------|--------|--------|
| **AUSSDA** (Austria) | All datasets require AUSSDA Scientific Use License; 0 downloads | ~1400 datasets lost |
| **ODISSEI** (Netherlands) | Metadata-only aggregator; `files` array always empty | ~213 datasets; discoverable via DANS/DataverseNL instead |
| **ČSDA** (Czech Republic) | All datasets use ČSDA restricted license; 0 downloads | Unknown count |
| **LiDA** (Lithuania) | All datasets use LiDA restricted license | ~175 interview datasets |
| **ISSDA** (Ireland) | All datasets use ISSDA End User License (non-open) | ~7 datasets |
| **QualidataNet** (Germany) | Metadata-only portal; no direct file downloads | ~143 datasets |
| **Qualiservice** (Germany) | Formal contract/usage agreement mandatory | Unknown count |
| **QualiBi** (Germany) | No public portal; 5 records "All rights reserved" via QualidataNet | 5 records |

**Documented in:** `datasources.csv` rows 4, 7, 8, 12, 13, 24, 25, 30

---

## 2. Newly Implemented Data Sources (2026-02-22)

These sources have been implemented and verified with live queries (20+ results each).

| Source | API Type | Query | Results | Harvest Status |
|--------|----------|-------|---------|----------------|
| **OSF** | JSON:API v2 | qualitative interview | 57 | Downloads working (CC BY items) |
| **FSD Finland** | OAI-PMH | interview | 373 | Category (A) CC BY 4.0 accepted; (B)+ rejected |
| **Internet Archive** | Advanced Search + Metadata API | oral history interview | 500 (capped) | Public domain detected from metadata; audio files catalogued |
| **Library of Congress** | JSON API | oral history transcript | 500 (capped) | PDFs downloading; "fully open content" recognized |

### Issues Found and Fixed

**ISSUE-1 (FSD):** Finnish license text `"(A) vapaasti käytettävissä"` not recognized by `license_is_open()`.
- **Fix:** Added `"(a) vapaasti"`, `"(a) openly available"`, `"cc by"` to `_OPEN_PHRASES` in `licensing.py`.

**ISSUE-2 (IA):** Many IA items lack explicit `licenseurl` even when public domain.
- **Fix:** Added fallback in `ia.py` to detect "public domain" from `rights`, `rights-info`, and description text.

**ISSUE-3 (LOC):** Rights text like `"This is non-restricted, fully open content..."` not matched.
- **Fix:** Added `"non-restricted"`, `"fully open content"`, `"no known restrictions"`, `"not aware of any copyright"` to `_OPEN_PHRASES`.

**Documented in:** `datasources.csv` rows 20–23 (status changed from "To implement" to "Implemented")

---

## 3. Search & Filtering Challenges

### 3.1 Search Result Cap

Hard-coded 500-result cap per query in both Dataverse and Figshare sources. Large repositories may have many more results, causing incomplete collection with no indication of what was missed.

- `src/harvester/sources/dataverse.py:21` — `_SEARCH_CAP = 500`
- `src/harvester/sources/figshare.py:20` — `_SEARCH_CAP = 500`

### 3.2 Resource Type Filtering Relies on Optional Metadata

The `kind_of_data` filter (`cli.py:162–172`) only activates when a dataset populates that field. Many Dataverse installations leave it empty, allowing non-qualitative resources (presentations, images, videos) to pass through unfiltered.

### 3.3 Keyword-Based Relevance Gate Is Fragile

Datasets without QDA files must match qualitative keywords in their description/keywords (`cli.py:174–183`). Problems:
- Relies on case-insensitive substring matching across ~80 keywords
- Partial matches cause false positives (e.g., "historical" matches "histor")
- Non-English datasets using untranslated terms may be missed
- Misspellings and variations are not caught

### 3.4 Size Cap Excludes Large Qualitative Files

Default 100 MB cap for non-QDA files (`cli.py:225–237`). Large `.txt`, `.pdf`, `.rtf`, or `.docx` files are skipped silently—only metadata is recorded with no easy way to identify size-skipped files.

### 3.5 File Type Whitelist Gaps

Only recognized file types are downloaded (`cli.py:192–200`). Missing from whitelists:
- Excel files (`.xlsx`, `.xls`) with qualitative data
- CSV/TSV data files
- Archive files (`.zip`, `.tar.gz`) containing multiple files
- Older proprietary QDA formats

---

## 4. Dataverse API Challenges

### 4.1 HTML Cleaning Is Regex-Based and Brittle

`dataverse.py:344–355` uses regex to fix broken HTML (`</ p`, `&#\s+(\d+);`, `xlink ">` fragments). No proper HTML parser is used, risking data corruption of legitimate content.

### 4.2 Deeply Nested Metadata Fields

Fields like `authorName.value`, `keywordValue.value`, and `softwareName.value` (`dataverse.py:126–227`) have complex nested structures. Missing or empty optional fields cause silent failures.

### 4.3 License Detection Fallback Chain

`dataverse.py:236–242` falls back from license block → termsOfAccess → termsOfUse. Different installations store license information in unexpected locations (e.g., Borealis uses `termsOfUse`; QDR uses "Standard Access").

### 4.4 Checksum Format Inconsistency

`dataverse.py:248–254` tries checksum wrapper, then MD5 directly, else empty string. Different installations use different checksum types (SHA-256, MD5) with no validation against downloaded files.

### 4.5 Date Field Parsing

`dataverse.py:207–226` expects `dateOfCollectionStart/End` and `timePeriodCoveredStart/End` but only checks the first entry in lists. No format validation, no timezone handling.

### 4.6 Harvested Dataset Exclusion

`dataverse.py:54` filters out `isHarvested:true` datasets. Some valid datasets may be incorrectly flagged, while metadata-only remote-indexed entries could slip through.

---

## 5. Figshare API Challenges

### 5.1 Type Filtering

`figshare.py:24–25, 76–78` skips "figure", "media", "code", "poster", and "presentation" types. Qualitative data may sometimes be classified under excluded types.

### 5.2 Hard-Coded Throttling

`figshare.py:43–48` enforces a 0.5-second delay between requests. No exponential backoff for 429 responses; delay may not match actual rate limits.

### 5.3 S3 Redirect Downloads

`figshare.py:211–245` relies on S3 redirects with `follow_redirects=True`. Temporary S3 URLs cannot be used for re-downloads. Redirect failures cause entire download failure.

### 5.4 Confidential / Metadata-Only Records

`figshare.py:121–127` returns empty file lists for confidential records. Cannot distinguish "no files" from "access denied."

### 5.5 Undefined MIME Types

`figshare.py:168–170` converts `"undefined"` MIME types to empty string, making file type filtering unreliable.

---

## 6. License Detection Challenges

**File:** `src/harvester/helpers/licensing.py`

### 6.1 Incomplete License Patterns

Only matches hardcoded prefixes: CC-BY variants, CC0, OD, MIT, Apache 2.0, "Standard Access", and "Etalab." Many open licenses are missing: AGPL, GPL, BSD, ISC, Unlicense.

### 6.2 License Text Variations

`"CC-BY-4.0"` vs `"CC BY 4.0"` vs `"CC-BY 4.0"` are handled inconsistently. Long `termsOfAccess` blobs are parsed with fragile regex (`licensing.py:43–47`).

### 6.3 Phrase Substring False Positives

`licensing.py:55` uses substring search. Phrases like "creative commons" may match even when the actual license is restrictive (e.g., "creative commons attribution-only, with additional restrictions").

---

## 7. Metadata Extraction Challenges

### 7.1 Semicolon-Delimited Multi-Value Fields

Multi-value fields (tags, keywords, software, authors) are joined with `"; "` (`cli.py:55–106`). Semicolons within individual values corrupt the data and prevent reliable parsing.

### 7.2 Inconsistent Author Representation

Authors appear as semicolon-joined strings from search results vs. lists from full metadata. No reliable way to distinguish multiple authors or track contact emails.

### 7.3 Description HTML Stripping

Both Dataverse and Figshare implement HTML stripping differently. Regex-based `_clean_html()` may corrupt legitimate HTML content.

### 7.4 Unverified API Checksums

`cli.py:94, 312` stores API-provided checksums separately from computed file hashes. No automatic verification; different formats (MD5 vs SHA-256).

---

## 8. Database & Schema Challenges

### 8.1 Manual Schema Migration

`engine.py:16–51` uses `ALTER TABLE` to add columns via a manually maintained `_EXTRA_COLUMNS` dict. No version control, no column removal/modification, no proper migration framework.

### 8.2 Unstructured Text Fields

All multi-value fields use TEXT columns (`models.py:44–59`). No array support means LIKE queries on concatenated strings and degraded search performance.

### 8.3 Nullable Metadata Fields

Many fields are nullable and frequently empty: author info, keywords, tags, software, geographic coverage, collection dates, time periods.

### 8.4 Timestamp Issues

`models.py:72–73` uses `default=datetime.utcnow` (evaluated at import time, not per-row). No last-modified timestamp; no tracking of when files become restricted/unavailable.

---

## 9. File Download & Deduplication Challenges

### 9.1 Post-Download Deduplication

`cli.py:278–283` detects duplicates by SHA-256 hash only after full download. No pre-download check, wasting bandwidth on duplicates. Large files (>50 MB) are slow to hash (`cli.py:272–275`).

### 9.2 Filename Extraction

`dataverse.py:366–372` extracts filenames from `Content-Disposition` headers. Fallback uses last URL segment (may be an ID). No validation for path traversal characters or encoding issues with non-ASCII filenames.

### 9.3 Restricted File Detection

`cli.py:239–248` checks a `restricted` flag and HTTP 403 status. API may incorrectly report files as unrestricted. No retry logic for 403 errors.

### 9.4 Partial Download Handling

`cli.py:250–269` catches `httpx.HTTPStatusError` and generic `Exception`. Partial downloads may corrupt local storage. No resume capability for interrupted downloads.

---

## 10. Storage & Path Challenges

### 10.1 Slug Collisions

`files.py:11–22` truncates slugs to 60 chars and splits on hyphens. Two datasets with similar titles may collide. No deterministic collision resolution.

### 10.2 Brittle Persistent ID Parsing

`cli.py:203–207` extracts IDs from DOIs/handles using string split/replace. Unexpected ID formats will break path construction.

### 10.3 Fixed Relative Paths

`config.yml:5–8`, `settings.py:14–16` hardcode downloads, output, database, and log paths relative to project root. No support for external or cloud storage.

---

## 11. Language & Internationalization Challenges

### 11.1 Limited Language Coverage

`config.yml:80–107` includes relevance keywords in only 7 languages: English, Dutch, Norwegian, German, Spanish, French, Portuguese. Chinese, Japanese, Russian, Arabic, and Hindi are not supported.

### 11.2 Mixed-Language Metadata

`dataverse.py:161–163` stores language as a semicolon-separated list. Cannot distinguish which language applies to which field (title vs. description).

### 11.3 Non-ASCII Corruption Risk

HTML entity handling in `dataverse.py:344–355` may corrupt non-ASCII characters. Broken entity workaround (`&#\s+(\d+);`) suggests encoding issues from some API responses.

---

## 12. Configuration & Deployment Challenges

### 12.1 Static Configuration

All settings in `config.yml` loaded at module import time (`settings.py`). Cannot change configuration without restarting. No environment variable overrides; no staging/production distinction.

### 12.2 Hardcoded Source Registry

`sources/__init__.py:7–21` hardcodes all sources. Cannot add, disable, or reconfigure sources without modifying code.

---

## 13. Missing Features

| Feature | Impact |
|---------|--------|
| **No resume/checkpointing** | Harvester must restart from beginning after failure |
| **No incremental updates** | Must re-harvest entire dataset; cannot fetch only new items |
| **No query-level retry** | If a query times out, its results are lost |
| **No data quality metrics** | No tracking of metadata completeness, missing checksums, or suspected duplicates |
| **Limited export formats** | Only CSV export (`cli.py:579–588`); no JSON, RDF, or standard metadata formats |

---

## 14. Optional Sources — Now Implemented (2026-02-22)

These 5 Dataverse-based sources have been added to the registry. All use the existing `DataverseSource` class with zero new code.

| Source | Query | Search Results | Harvest Status |
|--------|-------|---------------|----------------|
| **ADP Slovenia** | interview | 41 | All datasets `license: 'none'`; 0 downloads |
| **e-cienciaDatos (Madrid)** | entrevista | 35 | Downloads working; CC BY items found |
| **Peking University** | interview | 41 | Server 500 errors on file downloads; API unstable |
| **IZA Bonn** | interview | 40 | Mostly no-file or restricted-license (`IIL-1.0`) datasets |
| **SciELO Data (Brazil)** | pesquisa qualitativa | 255 | Downloads working; 22 files in 3-item test |

### Issues Found and Fixed

**ISSUE-4 (SciELO):** SciELO's nginx WAF blocks the default `python-httpx` User-Agent with 403 Forbidden.
- **Fix:** Added per-instance `headers` parameter to `DataverseSource.__init__()`. SciELO instance configured with a browser-style `User-Agent` string. Other Dataverse sources use the default httpx UA, since Sciences Po blocks browser-style UAs.
- **File:** `dataverse.py` — added `_BROWSER_UA` constant and `headers` kwarg; `__init__.py` — SciELO entry uses `headers={"User-Agent": _BROWSER_UA}`.

**ISSUE-5 (ADP Slovenia):** All 41 datasets have `license: 'none'`, indicating the Slovenian repository does not populate the license field in its Dataverse installation.
- **Status:** No fix possible — this is a data quality issue on the source side. Datasets may still be open but cannot be verified programmatically.

**ISSUE-6 (Peking University):** File download API returns HTTP 500 errors for all attempted downloads.
- **Status:** No fix possible — this is a server-side issue on Peking University's Dataverse installation. Search and metadata APIs work correctly.

**ISSUE-7 (IZA Bonn):** Most datasets have no attached files or use the proprietary `IIL-1.0` license (not recognized as open).
- **Status:** Expected behavior — IZA is primarily a labor economics data archive with limited open qualitative content.

**Documented in:** `datasources.csv` rows 23a–23e (status changed from "Optional" to "Implemented")
