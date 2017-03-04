# coding: utf-8

from bigquery import get_client
import pandas as pd

json_key = "pricespectrum-ddb205dae4ad.json"
client = get_client(json_key_file=json_key, readonly=True)

sql = "SELECT delngDe as Date, aucSeNm as TradeType, whsalMrktNewNm as Market, insttNewNm as Agent, shipmntSeNm as ShippingType, stdMtcNewNm as Origin, stdSpciesNm as Species, stdFrmlcNewNm as Package, delngPrut as Unit, stdUnitNewNm as UnitScale, stdQlityNewNm as Grade, delngQy as Quantity, sbidPric as Price FROM [pricespectrum-159700:auction_price.realtime] WHERE stdPrdlstNewCode = '0909' AND RAND() < 0.05 LIMIT 1000"

job_id, _results = client.query(sql)

print(job_id)

complete, row_counts = client.check_job(job_id)

print(complete, row_counts)

results = client.get_query_rows(job_id)

sample = pd.DataFrame(results)

sample.to_csv("samples/sample.csv", header = True, index = False, encoding = "utf-8")

