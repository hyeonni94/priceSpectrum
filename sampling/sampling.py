# coding: utf-8

from bigquery import get_client
import pandas as pd
import time

json_key = "pricespectrum-ddb205dae4ad.json"
client = get_client(json_key_file=json_key, readonly=True)

yearMonth = '201502'

sql_base = "SELECT delngDe as Date, whsalMrktNewNm as Market, insttNewNm as Agent, shipmntSeNm as ShippingType, stdMtcNewNm as Origin, stdSpciesNm as Species, stdFrmlcNewNm as Package, delngPrut as Unit, delngQy as Quantity, sbidPric as Price, INTEGER(CEIL(sbidPric / FLOAT(delngPrut))) as UnitPrice FROM  [pricespectrum-159700:auction_price.realtime] WHERE stdPrdlstNewCode = '0909' AND aucSeNm = '경매' AND stdSpciesNm IN ('설향', '장희') AND stdMgNewNm = '기타' AND stdQlityNewNm = '특' AND INTEGER(CEIL(sbidPric / FLOAT(delngPrut))) <= 100000 AND delngDe CONTAINS '" + yearMonth + "' AND stdFrmlcNewNm = '"

sql_box = sql_base + "상자" + "'"
sql_etc = sql_base + "기타" + "'"

print("sql_box is...\n" + sql_box)
print("sql_etc is...\n" + sql_etc)

job_id, _results = client.query(sql_box)
time.sleep(60)
print(job_id)
complete, row_counts = client.check_job(job_id)
time.sleep(60)
print(complete, row_counts)
results_box = client.get_query_rows(job_id)
time.sleep(60)
sample_box = pd.DataFrame(results_box)
time.sleep(60)
sample_box.to_csv("samples/sample" + yearMonth + "box.csv", header = True, index = False, encoding = "utf-8")
time.sleep(60)

job_id, _results = client.query(sql_etc)
time.sleep(60)
print(job_id)
complete, row_counts = client.check_job(job_id)
time.sleep(60)
print(complete, row_counts)
results_etc = client.get_query_rows(job_id)
time.sleep(60)
sample_etc = pd.DataFrame(results_etc)
time.sleep(60)
sample_etc.to_csv("samples/sample" + yearMonth + "etc.csv", header = True, index = False, encoding = "utf-8")
