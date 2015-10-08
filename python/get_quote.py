#! /usr/bin/python

import sys
import MySQLdb
import urllib2
import json
import time

def get_quote(exchange):
  try:   
    prefix = "http://finance.google.com/finance/info?client=ig&q="    
    conn, cursor = get_DBconnection()
    tbl_name = "STAGING_SEC_QUOTES"
    fxd_qry = "REPLACE INTO " + tbl_name + " VALUES"
    del_qry = "DELETE FROM STAGING_SEC_QUOTES"
    ins_qry = """REPLACE INTO SEC_PRICES(TICKER_ID,QUOTE_DATE,LT_PRICE,PREV_CLO_PRICE,EXCHANGE_ID) 
		  SELECT SI.ID,S.QUOTE_DATE,S.LT_PRICE,S.PREV_CLO_PRICE,E.ID FROM STAGING_SEC_QUOTES S 
		  JOIN EXCHANGE E ON S.`EXCHANGE` = E.NAME
		  JOIN SEC_IDENTIFIERS SI ON SI.TICKER = S.COMPANY_NAME"""
    qry = ''
    cursor.execute(del_qry)
    get_stocks_qry = "select group_concat(':',TICKER) from SEC_IDENTIFIERS group by id%20" 
    cursor.execute(get_stocks_qry)
    stocks = cursor.fetchall()
    for s in stocks:
        url = prefix+"%s"%(s[0].replace(':','NSE:').replace("&","%26"))
	u = urllib2.urlopen(url)
        content = u.read()
        obj = json.loads(content[3:])
	for i in range(0,len(obj)):
		exch = obj[i]['e']
        	tckr = obj[i]['t']
        	print "[" + exch + ":" + tckr + "]"
		qdate = str(obj[i]['lt_dts'])[0:10]
        	lt_price = obj[i]['l_fix']
		pcl_price = obj[i]['pcls_fix']
        	if exch is not None and tckr is not None and qdate <> '' and qdate is not None and lt_price is not None and pcl_price is not None:
			qry = qry + "('" + exch + "','" + tckr + "','" + qdate + "'," + lt_price + "," + pcl_price + ")," 
	cursor.execute(fxd_qry + qry[:-1])
	qry = ''
	print "Number of prices inserted: %d" % cursor.rowcount
    cursor.execute(ins_qry)
    cursor.close()
    conn.commit()
    conn.close()
  except urllib2.HTTPError:
    print "Quote not available"
    pass
 
def get_DBconnection():
    try:
        conn = MySQLdb.connect("localhost","root","","UGFRHE")
        cursor = conn.cursor()
        return conn, cursor
    except Exception as ex:
        print 'Exception:',str(ex)
        sys.exit(2)
        
if __name__ == "__main__":
    get_quote("NSE")
