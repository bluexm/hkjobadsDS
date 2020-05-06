import bs4
import re
import requests as rqs
import csv
import datetime 
import pandas as pd
#import Levenshtein as levs 
#import scraperwiki as ws *** DEPRECATED *** 
import pdb

## user params 
NBPAGESMAX = 10 	# number of pages for search results 
RECORD_EXCEL = False # only not previously recorded ads are stored in the excel file 
RECORD_CSV = False  # all search results are stores in the CSV 
RECORD_DB = True 	# record in DB with wikiscraper (for morph.io)
RECORD_ALWAYSWRITE = False #always write record even if already recorded 
USE_SCRAPERWIKI = False # if recorddb = True then use scraperwiki *** DEPRECATED *** 
DB_FILE = "data.sqlite"
DB_TITLES = ["timestamp","scrping_dt","ad_cie_indeed","ad_jobtitle_indeed","search_ad_url","ad_url","ad_jobdate",  \
				"ad_jobtitle","ad_jobcie","ad_jobdes","ad_email"]
URL ='https://www.indeed.hk/jobs?q=Data+Scientist'

if RECORD_DB and not USE_SCRAPERWIKI:
	import sqlite3
	from sqlite3 import Error
	try:
		CONNEXION = sqlite3.connect(DB_FILE)
		print("connexion with DB successful, using SQLlite ", sqlite3.version)
	except Error as e:
		print(e)

def dict_value(tuple):
	return tuple[-1]

exectime = datetime.datetime.now()

# init csv 
if RECORD_CSV:
	csvfile = open('scrape_indeed_'+ exectime.strftime("%Y%m%d")+'.csv', 'w', newline='',encoding="utf-8")
	writer = csv.writer(csvfile, delimiter=';', quotechar="'", quoting=csv.QUOTE_MINIMAL)

# init dataframe 
if RECORD_EXCEL:
	excelDBfilename = 'scraping_indeed.xlsx'
	df = pd.read_excel(excelDBfilename, 'Sheet1', index_col=None, na_values=['NA'])
	
if RECORD_DB and not USE_SCRAPERWIKI: 
	#pdb.set_trace()
	dfdb = pd.DataFrame(columns=DB_TITLES)
	# or read column titles from database 
	# try:
	# 	dfdb = pd.read_sql("select * from indeed_ads", CONNEXION)
	# 	dfdb = dfdb[1:]
	# except:
	# 	print("database empty ; creating table")
		
## single ad page scrapers 
def parse_workinginhongkong(pdata):
	adtree = bs4.BeautifulSoup(pdata, 'html.parser')
	if adtree.find("div", class_="section_header") ==None: 
		return 'NA','NA','NA','NA','NA'
	else: 
		addate = ''.join([s for s in adtree.find("div", class_="section_header").find("div", class_="date").stripped_strings])
		adtitle = ''.join([s for s in adtree.find("div", class_="section_header").find("h1", class_="title").stripped_strings])
		adcompany = adtree.find("div", class_="section_header").p.a.string

		a = [s.stripped_strings for s in adtree.find("div", class_="section_content").find_all("p")]
		adjobdes = ''.join([''.join(s) for s in a])
		ademail = re.findall('[.\w]*@+\w+.com',pdata)
	return addate, adtitle, adjobdes, adcompany, ademail

def parse_indeed(pdata):
	adtree = bs4.BeautifulSoup(pdata, 'html.parser')
	root= adtree.find("div", class_="jobsearch-ViewJobLayout-jobDisplay")
	#pdb.set_trace()

	if root==None: 
		return 'NA','NA','NA','NA','NA'
	else: 
		addate = str(list(root.find("div", class_="jobsearch-JobMetadataFooter").stripped_strings)[1])
		adtitle = str(''.join([s for s in root.find("div", class_="jobsearch-JobInfoHeader-title-container").stripped_strings]))
		adcompany = str(root.find("div", class_="icl-u-lg-mr--sm").string)
		adjobdes = str(''.join([s for s in root.find("div", id="jobDescriptionText").stripped_strings]))
		ademail = str(';'.join(re.findall("[.\w]*@+\w+.com",pdata)))
		if ademail =='': ademail='NA'
	return addate, adtitle, adjobdes, adcompany, ademail
	
print("start of scraping ---------------------------------------------------------")
res=[]
for i in range(NBPAGESMAX):
	try:
		cURL= URL + ('&start={:d}'.format(10*i) if i>0 else '')
		print('searching  ', ) 
		#r = ht.request('GET',URL+ str(i+1)+'0')
		r = rqs.get(cURL)
		#tx = r.data.decode("utf-8","ignore")
		tx = r.content
		#print(tx)
		
		## scraping 
		tree = bs4.BeautifulSoup(tx, 'html.parser')
		#print(tree.prettify())
		content = tree.find_all("div",class_=re.compile("row")) #get all divs with class "row" 
		
		## iterates on all search results 
		for c in content:
			rowres=[exectime.timestamp(), exectime.strftime("%Y-%m-%d %H:%M:%S")]
			#rowres.append(c.find_all("span",class_="company")[0].get_text())
			#records company's listed on search
			#pdb.set_trace()
			rowres.append(str(''.join([s for s in c.find_all("span",class_="company")[0].stripped_strings]))) #company name 
			jobtitle= str(c.find_all("a",class_="jobtitle")[0].get_text())
			rowres.append(jobtitle.strip())	#job title 
			adlink = 'https://www.indeed.hk'+ c.find_all("a",class_="jobtitle")[0]['href']  #adlink 
			rowres.append(adlink)
			
			## get ad detailed infos
			print('ad page ', adlink[:50] + '...', end='')
			ad = rqs.get(adlink)
			adshorturl = ad.url
			rowres.append(adshorturl)
	
			##branch on correct parser
			addate, adtitle, adcompany, adjobdes,  ademail = parse_indeed(ad.text)
			rowres += [addate, adtitle, adjobdes, adcompany, ademail]

			## check if ad is already here or not
			dorecord=True

			'''#commented since levenshtein not available in morph.io and check is done vs df coming from Excel worksheet 
			for k in df['search_ad_url']:
				#Levenshtein not in Morph.io --> commented 	
				#ldist = levs.distance(adlink,k)
				#ldistratio = (len(k)-ldist)/len(adlink)
				##print(ldist, ldistratio,end = ' ')
				if adlink == k:  # if urls are the same don't store 
				#if ldistratio>0.95:
					print("adlink", adlink, " k  ; ",k)
					dorecord=False
					break
			'''

			for k in dfdb['search_ad_url']:
				#Levenshtein not in Morph.io --> commented 	
				#ldist = levs.distance(adlink,k)
				#ldistratio = (len(k)-ldist)/len(adlink)
				##print(ldist, ldistratio,end = ' ')
				#if ldistratio>0.95:
				if adlink == k:  # if urls are the same don't store 
					print("adlink already recorded")
					dorecord=False
					break
			
			## store and display results 
			#print(' ; '.join([str(s) for s in rowres]))
			if RECORD_CSV:
				print("write csv")
				writer.writerow(rowres)
				#res.append(rowres)
			if dorecord or RECORD_ALWAYSWRITE:
				# in dataframe for excel 
				if RECORD_EXCEL:
					print("record to Excel")
					rowres2 = rowres
					rowres2[4]=rowres2[4][:255]
					df = df.append(pd.DataFrame([rowres2],columns=df.columns))
				# in SQLlite 
				if RECORD_DB:
					if USE_SCRAPERWIKI:
						print("record in wiki db")
						ws.sqlite.save(unique_keys=['ad_url'], table_name="indeed_ads", data={x:y for (x,y) in zip(DB_TITLES,rowres)} )
					else:
						print("record in local db")
						#pdb.set_trace()
						dfdb = dfdb.append(pd.DataFrame([rowres],columns=dfdb.columns), ignore_index=True)
	except: 
		pass
		
print("end of scraping ---------------------------------------------------------")

if RECORD_DB and not USE_SCRAPERWIKI:
	#pdb.set_trace()
	dfdb.to_sql('indeed_ads',CONNEXION,if_exists='append', index=False)
	CONNEXION.close()

if RECORD_EXCEL:
	df.to_excel(excelDBfilename, sheet_name='Sheet1', index=False)

if RECORD_CSV:
	csvfile.close()