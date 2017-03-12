# coding: utf-8

import requests
import xml.etree.ElementTree as ET
import datetime
import calendar
import math
import sqlite3
import re
import sys
import logging
from time import sleep

year = int(sys.argv[1])
month = int(sys.argv[2])
lastDayOfMonth = calendar.monthrange(year, month)[1]
begin = datetime.date(year, month, 1)
end = datetime.date(year, month, lastDayOfMonth)


logging.basicConfig(filename='./log/' + str(year) + format(month, '02d') + '.log', level=logging.DEBUG)


conn = sqlite3.connect("/media/wooyol/storage/dataCenter/auctionPrice/price" + str(year) + format(month, '02d') + ".db")
cur = conn.cursor()


BaseURL = "http://data.mafra.go.kr:7080/openapi/"
AuthKey = "6fba9600d737080f5b17571285e1ba13a557e2914b037c6e6c22df87f7003365"
TailURL = "/xml/Grid_20161229000000000478_1/"


def getTotalCnt(date) :
    url = "%s%s%s%s/%s?AUCNG_DE=%s" % (BaseURL, AuthKey, TailURL, '1', '1', date)
    response = requests.get(url)
    root = ET.fromstring(response.content)
    count = root.find("totalCnt").text
    logging.info("total count in " + date + " is " + count)
    return count


def doRequest(url) :
    try :
        return requests.get(url)
    except requests.exceptions.ConnectionError :
        sleep(10)
        return doRequest(url)


def submitRequest(url) :
    response = doRequest(url)
    try :
        root = ET.fromstring(response.content)
    except ET.ParseError e :
        print(str(e) + "\nparse error in " + url)
        logging.info("parse error in " + url)
    rows = root.findall('row')
    if rows is not None :
        for row in rows :
            sql = "insert into PRICE(ORGNO, PRDLST_CD, SANJI_NM, PRICE, PRDLST_NM, SPCIES_CD, STNDRD, RISENO, DELNG_QY, BIDTIME, SANJI_CD, GRAD_CD, CPR_NM, PBLMNG_WHSAL_MRKT_CD, AUCNG_DE, SPCIES_NM, STNDRD_CD, GRAD, PBLMNG_WHSAL_MRKT_NM, DELNGBUNDLE_QY, ROW_NUM, CPR_CD) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            cur.execute(sql, (int(re.sub("\D", "", row.findtext('ORGNO'))), row.findtext('PRDLST_CD'), row.findtext('SANJI_NM'), int(re.sub("\D", "", row.findtext('PRICE'))), row.findtext('PRDLST_NM'), row.findtext('SPCIES_CD'), row.findtext('STNDRD'), int(re.sub("\D", "", row.findtext('RISENO'))), int(re.sub("\D", "", row.findtext('DELNG_QY'))), row.findtext('BIDTIME'), row.findtext('SANJI_CD'), row.findtext('GRAD_CD'), row.findtext('CPR_NM'), row.findtext('PBLMNG_WHSAL_MRKT_CD'), row.findtext('AUCNG_DE'), row.findtext('SPCIES_NM'), row.findtext('STNDRD_CD'), row.findtext('GRAD'), row.findtext('PBLMNG_WHSAL_MRKT_NM'), float(re.sub("[^0123456789\.]", "", row.findtext('DELNGBUNDLE_QY'))), int(re.sub("\D", "", row.findtext('ROW_NUM'))), row.findtext('CPR_CD')))
            conn.commit()

    logging.info(url + " is complete")


def getDateSeries(start, end, step = datetime.timedelta(1)) :
    curr = start
    dateList = []
    while curr <= end :
        dateList.append(curr.strftime('%Y%m%d'))
        curr += step
    return dateList


def getIndexSeries(date) :
    count = getTotalCnt(date)
    number = int(math.ceil(float(count) / 1000))
    indexSeries = []
    for i in range(0, number) :
        index = {}
        index['startIndex'] = str(1 + 1000 * i)
        index['endIndex'] = str(1000 + 1000 * i)
        indexSeries.append(index)
    return indexSeries


def getUrlSeries(startDate, endDate) :
    urlSeries = []
    dateSeries = getDateSeries(startDate, endDate)
    for date in dateSeries :
        indexSeries = getIndexSeries(date)
        for index in indexSeries :
            url = "%s%s%s%s/%s?AUCNG_DE=%s" % (BaseURL, AuthKey, TailURL, index['startIndex'], index['endIndex'], date)
            urlSeries.append(url)
    return urlSeries


def main(begin, end) :
    urlSeries = getUrlSeries(begin, end)
    for url in urlSeries :
        submitRequest(url)
    conn.close()



main(begin, end)
