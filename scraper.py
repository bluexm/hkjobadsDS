import bs4
import re
import requests as rqs
import csv
import datetime 
import pandas as pd 
#import Levenshtein as levs 
#import scraperwiki as ws
import pdb
 
## user params 
URL ='https://www.indeed.hk/jobs?q=Data+Scientist&start='
NBPAGESMAX = 10 	# number of pages for search results 
RECORD_EXCEL = True # only not previously recorded ads are stored in the excel file 
RECORD_CSV = True  # all search results are stores in the CSV 
RECORD_DB = False 	# record in DB with wikiscraper (for morph.io)
USE_SCRAPERWIKI = False # if recorddb = True then use scraperwiki or SQLlite directly 
DB_FILE = "data.sqlite"
DB_TITLES = ["epoch","scrping_dt","ad_cie_indeed","ad_jobtitle_indeed","search_ad_url","ad_url","ad_jobdate","ad_jobtitle","ad_jobcie","ad_jobdes","ad_email"] 	

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
	
if RECORD_DB and USE_SCRAPERWIKI: 
	# TODO : add connection to scraper wiki to read database 
	pass
else: # record in lolcal database 
	#pdb.set_trace()
	try:
		dfdb = pd.read_sql("select * from indeed_ads", CONNEXION)
		dfdb = dfdb[1:]
	except:
		dfdb = pd.DataFrame(columns=DB_TITLES)
		print("database empty ; creating table")
	
""" single ad page scrapers """
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
		ademail = re.findall("[.\w]*@+\w+.com",pdata)
	return addate, adtitle, adjobdes, adcompany, ademail

def parse_indeed(pdata):
	adtree = bs4.BeautifulSoup(pdata, 'html.parser')
	root= adtree.find("table", id="job-content")
	#pdb.set_trace()
	if root==None: 
		return 'NA','NA','NA','NA','NA'
	else: 
		addate = str(''.join([s for s in root.find("span", class_="date").stripped_strings]))
		adtitle = ''.join([s for s in root.find("b", class_="jobtitle").stripped_strings])
		adcompany = root.find("span", class_="company").string
		adjobdes = ''.join([s for s in root.find("span", id="job_summary").stripped_strings])
		ademail = re.findall("[.\w]*@+\w+.com",pdata)
		if ademail ==[]: ademail='NA'
	return addate, adtitle, adjobdes, adcompany, ademail
	
def parse_gobee(pdata):
	adtree = bs4.BeautifulSoup(pdata, 'html.parser')
	adtitle = adtree.find("div", class_="job-header").find("h1").string
	if adtitle==None: 
		return 'NA','NA','NA','NA','NA'
	else: 
		st = tree.find("script", type="application/ld+json").string.title()
		st = re.findall("Dateposted.*,",st)[0]
		p = st.find(",")
		addate = st[p-11:-2]
		adcompany = "gobeebike"
		adjobdes = ' '.join([s for s in adtree.find("div", class_="description").stripped_strings])
		ademail = "NA"

	return addate, adtitle, adjobdes, adcompany, ademail
	
def parse_classywheeler(pdata):
	return '','','','',''

	
## NOT FINISHED !!!
def parse_whub(pdata):
	pdb.set_trace()
	adtree = bs4.BeautifulSoup(pdata, 'html.parser')
	root = adtree.find("h1", itemprop="title")
	if root==None: 
		return 'NA','NA','NA','NA','NA'
	else:
		addate = ''.join([s for s in adtree.find("p", itemprop="Created on :").stripped_strings])
		adtitle = ''.join([s for s in adtree.find("div", class_="job-banner").findall("p")[2].stripped_strings])
		adcompany = ''.join([s for s in adtree.find("span", itemprop="hiringOrganization").stripped_strings])
		adjobdes = ''.join([s for s in adtree.find("p", class_="job-description").stripped_strings])
		ademail = re.findall("[.\w]+@+\w+.com",pdata)
		if ademail ==[]: ademail='NA'
	return addate, adtitle, adjobdes, adcompany, ademail

def parse_efinancialcareers(pdata):
	adtree = bs4.BeautifulSoup(pdata, 'html.parser')
	root = adtree.find("h1", itemprop="title")
	#pdb.set_trace()
	if root==None: 
		return 'NA','NA','NA','NA','NA'
	else: 
		addate = ''.join([s for s in adtree.find("span", itemprop="datePosted").stripped_strings])
		adtitle = ''.join([s for s in adtree.find("h1", itemprop="title").stripped_strings])	
		adcompany = ''.join([s for s in adtree.find("span", itemprop="hiringOrganization").stripped_strings])
		adjobdes = ''.join([s for s in adtree.find("div", itemprop="description").stripped_strings])
		ademail = re.findall("[.\w]+@+\w+.com",pdata)
		if ademail ==[]: ademail='NA'
	return addate, adtitle, adjobdes, adcompany, ademail
	
""" scraping from indeed.hk """ 

## declare parser functions to use according to site url (or word in the url)
adparsers = {	"workinginhongkong": parse_workinginhongkong, 
				"indeed":parse_indeed,
				"gobee":parse_gobee,
				"efinancialcareers":parse_efinancialcareers,
				#"whub":parse_whub,
				#"classywheeler": parse_classywheeler, 
				#"efinancialcareers": parse_efinancialcareers
			}

res=[]
for i in range(NBPAGESMAX):
	try:
		print('searching  ', URL+ str(i+1)+'0')
		#r = ht.request('GET',URL+ str(i+1)+'0')
		r = rqs.get(URL+ str(i+1)+'0')
		#tx = r.data.decode("utf-8","ignore")
		tx = r.content
		#print(tx)
		
		##scraping 
		tree = bs4.BeautifulSoup(tx, 'html.parser')
		#print(tree.prettify())
		content = tree.find_all("div",class_=re.compile("row"))
		#print(content)
		
		## iterates on all search results 
		for c in content:
			rowres=[exectime.timestamp(), exectime.strftime("%Y-%m-%d %H:%M:%S")]
			#rowres.append(c.find_all("span",class_="company")[0].get_text())
			#records company's listed on search
			#pdb.set_trace()
			rowres.append(''.join([s for s in c.find_all("span",class_="company")[0].stripped_strings]))
			rowres.append(c.find_all("a",class_="turnstileLink")[0].get_text())
			adlink = 'https://www.indeed.hk'+ c.find_all("a",class_="turnstileLink")[0]['href']
			rowres.append(adlink)
			
			## get ad detailed infos 
			print('ad page ', adlink)
			ad = rqs.get(adlink)
			adshorturl = ad.url
			rowres.append(adshorturl)

			##branch on correct parser
			addate, adtitle, adcompany, adjobdes,  ademail = 'NotParsed','NotParsed','NotParsed','NotParsed','NotParsed'
			for k in adparsers.keys():
				if re.findall(k,adshorturl):
					addate, adtitle, adcompany, adjobdes,  ademail = adparsers[k](ad.text)
					break

			rowres += [addate, adtitle, adjobdes, adcompany, ademail]

			## check if ad is already here or not
			dorecord=True
			
			'''' 
			commented since levenshtein not avialbale in morph.io and check is done vs df coming from Excel worksheet 
			for k in df['search_ad_url']:
				#Levenshtein not in Morph.io --> commented 	
				#ldist = levs.distance(adlink,k)
				#ldistratio = (len(k)-ldist)/len(adlink)
				##print(ldist, ldistratio,end = ' ')
				#if ldistratio>0.95:
				if adlink == k:  # if urls are the same don't store 
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
					print("adlink already recorded : ")
					dorecord=False
					break
			
			## store and display results 
			#print(' ; '.join([str(s) for s in rowres]))
			if RECORD_CSV:
				print("write csv")
				writer.writerow(rowres)
				#res.append(rowres)
			if dorecord:
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
						import pdb
						#pdb.set_trace()
						dfdb = dfdb.append(pd.DataFrame([rowres],columns=dfdb.columns), ignore_index=True)

	except: 
		pass
		
print("end of scraping ---------------------------------------------------------")

if RECORD_EXCEL:
	df.to_excel(excelDBfilename, sheet_name='Sheet1', index=False)
if RECORD_DB and not USE_SCRAPERWIKI:
	dfdb.to_sql('indeed_ads',CONNEXION,if_exists='append', index=False)
if RECORD_CSV:
	csvfile.close()
if CONNEXION:
	CONNEXION.close()