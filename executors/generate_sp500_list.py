# generate_sp500_list.py  (≈15 lines, no extra deps)
import pandas as pd, sys, textwrap, subprocess, json, os, re, urllib.request, io

URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

html = urllib.request.urlopen(URL).read()
# pandas can pull the first table on the page in one line
df = pd.read_html(html, header=0)[0]

symbols = df["Symbol"].tolist()
symbols = [s.strip().upper() for s in symbols]

with open("sp500_symbols.txt", "w", encoding="utf-8") as f:
    f.write("\n".join(symbols))

print(f"Wrote {len(symbols)} symbols to sp500_symbols.txt")
