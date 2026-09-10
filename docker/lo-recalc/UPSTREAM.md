# Vendored file provenance

`open_spreadsheet.py` is a copy of `evaluation/open_spreadsheet.py` from
https://github.com/RUCKBReasoning/SpreadsheetBench-2 at commit
d83edf90d891a1f873f04b92152503a57717496a (2026-06-29; the file is unchanged
upstream as of 5c16026, 2026-08-22). Upstream MD5: 865e81090cfed57ae51ab25b55e3b9fa.

Local modification: a `--suffix` option (default `output.xlsx`, the upstream
behaviour) selects which workbooks are processed; `--suffix .xlsx` processes
every workbook, which the upstream README requires for the dataset's input and
golden files but the upstream script cannot do. Lock files (`~$*`) are skipped.

The image builds on `debian:trixie-slim` for LibreOffice 25.2. Upstream pins no
LibreOffice version; versions before 24.8 lack XLOOKUP and cache `#NAME?` for
it, which changes every dependent value. `lo-version.txt` in a parity directory
records the version used.

The upstream repository contains no license file as of the vendored commit.
The copy is made with attribution for interoperability with the benchmark's
published evaluation protocol; replace or relicense if upstream clarifies
licensing.
