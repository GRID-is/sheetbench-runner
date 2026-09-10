FROM debian:trixie-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      libreoffice-calc python3-uno python3-tqdm fonts-liberation \
 && rm -rf /var/lib/apt/lists/* \
 && soffice --version

ENV LC_ALL=C.UTF-8

COPY open_spreadsheet.py /opt/open_spreadsheet.py

# CMD (deliberately not ENTRYPOINT) so callers can override the command,
# e.g. `docker run --rm lo-recalc soffice --version` for provenance.
CMD ["python3", "/opt/open_spreadsheet.py", "--dir_path", "/data"]
