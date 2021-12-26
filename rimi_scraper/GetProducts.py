#---------------------------------------------------LIBRARIES------------------------------------------------

import pandas as pd
import time
import pyodbc
import datetime
from requests_html import AsyncHTMLSession, HTML 
import demjson

#-----------------------------------------------------SESSION------------------------------------------------

s = AsyncHTMLSession()

#--------------------------------------------------SQL CONNECTIONS-------------------------------------------

db_conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=DESKTOP-LP9CHI3;'
                      'Database=rimi_data;'
                      'Trusted_Connection=yes;')
cursor = db_conn.cursor()

#--------------------------------------------------SQL QUERIES-----------------------------------------------

select_all_links = """

select product_link from rimi_data.e_rimi.product_links order by scrape_timestamp asc

"""

product_insert_query = """

insert into rimi_data.e_rimi.product_data (
    snapshot_date
    ,product_code
    ,product_name
    ,product_category
    ,product_subcategory
    ,product_type
    ,price_per_item
    ,item
    ,old_price
    ,is_discounted
    ,discount_percentage
    ,discount_start_date
    ,discount_end_date
    ,price_per_unit
    ,unit
    ,minimum_amount
    ,origin_country
    ,brand
    ,producer
    ,amount
    ,alcohol
    ,extract_timestamp
    ,product_link
)

values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

"""

#-------------------------------------------------PRODUCT DETAIL FUNCTIONS---------------------------------------------

#the function parses JavaScipt objects, tries to return it as JSON if it is not corrupted and get to HTML within it
def GetProductHtml(html_doc):
    
    script_text = html_doc.find('div.cart-layout__main', first=True).find('script')[1].text
        
    try:
        clean_text = script_text.split('; C')[0].split('Config.product_details_page = ')[1]
        product_json = demjson.decode(clean_text)
        html_from_json = HTML(html=product_json['tabs'][0]['html'])
        product_data = html_from_json.find('ul.list', first=True).html
        return product_data
    except:
        try:
            clean_text = script_text.split(';')[0].split('Config.product_details_page = ')[1]
            product_json = demjson.decode(clean_text)
            html_from_json = HTML(html=product_json['tabs'][0]['html'])
            product_data = html_from_json.find('ul.list', first=True).html
            return product_data
        except:
            return None

#parses the HTML component from JavaScript object and returns product information
def GetProductInfo(html_doc):

    try:
        script_html = HTML(html=GetProductHtml(html_doc))

        product_info = {}
        for i in range(len(script_html.find('ul.list', first=True).find('span'))):
            category = script_html.find('ul.list', first=True).find('span')[i].text
            item = script_html.find('ul.list', first=True).find('p')[i].text
            product_info[category] = item  
        return product_info
    except:
        return None

#parses the HTML returned by the rendered error handler
def GetProductInfoRendered(html_doc):

    try:
        product_info = {}
        for i in range(len(html_doc.find('ul.list', first=True).find('span'))):
            category = html_doc.find('ul.list', first=True).find('span')[i].text
            item = html_doc.find('ul.list', first=True).find('p')[i].text
            product_info[category] = item  
        return product_info
    except:
        return None

#returns the country from item details
def GetCountry(dictionary):
    try:
        country = dictionary['Country of origin']
        return country
    except:
        return None

#returns producer from item details
def GetProducer(dictionary):
    try:
        producer = dictionary['Producer']
        return producer
    except:
        return None

#returns the brand from item details
def GetBrand(dictionary):
    try:
        brand = dictionary['Brand']
        return brand
    except:
        return None

#returns amount from item details
def GetAmount(dictionary):
    try:
        amount = dictionary['Amount']
        return amount
    except:
        return None

#returns alcohol content from item details
def GetAlcohol(dictionary):
    try:
        alcohol = dictionary['Alcohol volume']
        return float(alcohol[0:-1]) / 100
    except:
        return None

#checks whether the old price exists
def GetOldPrice(html_doc):
    try:
        old_price = float(html_doc.find('p.price__old-price', first=True).text[0:-1].replace(',', '.'))
        return old_price
    except:
        return None

#extract discount periods if they exist
def GetDiscountPeriods(html_doc):
    try:
        date_phrase = html_doc.find('p.notice', first=True).text.split()
        start_date = datetime.datetime.strptime(date_phrase[-3][0:-1], '%d.%m.%Y').date()
        end_date = datetime.datetime.strptime(date_phrase[-1][0:-1], '%d.%m.%Y').date()
        return start_date, end_date
    except:
        return None, None

#extracts price per item
def GetPricePerItem(html_doc):
    try:
        euros = html_doc.find('div.product__main-info', first=True).find('div.price', first=True).find('span', first=True).text
        cents = html_doc.find('div.product__main-info', first=True).find('div.price', first=True).find('sup', first=True).text
        price_per_item = float('.'.join([euros,cents]))
        return price_per_item
    except:
        return None

def GetItem(html_doc):
    try:
        item = html_doc.find('div.product__main-info', first=True).find('div.price', first=True).find('sub', first=True).text
        return item
    except:
        return None

def GetPricePerUnit(html_doc):
    try:
        price_per_unit, currency, measure = html_doc.find('div.product__main-info', first=True).find('p.price-per', first=True).text.split(' ')
        price_per_unit = float(price_per_unit.replace(',', '.'))
        unit = "".join([currency, measure])
        return price_per_unit, unit
    except:
        return None, None

def GetMinimumAmount(html_doc):
    try:
        minimum_amount = html_doc.find('span.counter__number', first=True).text
        return minimum_amount
    except:
        return None

def GetDiscount(current_price, old_price, if_discounted):
    if if_discounted == 1:
        discount_pct = round( 1- (current_price / old_price), 2 )
        return discount_pct
    else:
        return None

def GetProductCategories(html_doc):
    try:
        product_category, product_subcategory, product_type = html_doc.find('div.section-header__container', first=True).text.split('\n')
        return product_category, product_subcategory, product_type
    except:
        return None, None, None

def GetProductName(html_doc):
    try:
        product_name = html_doc.find('div.product__main-info', first=True).find('h3.name', first=True).text
        return product_name
    except:
         None

#------------------------------------------------------PRODUCT DETAILS SCRAPE FUNCTION-------------------------------------------------------------

#async function to scrape several links at a time
async def product_details(url, header, snapshot_date):
  
    try:
        r = await s.get(url, headers=header)
    except Exception as e:
        print(e)
        r = None

    #product information variables
    product_code = url.split('/')[-1]
    product_category, product_subcategory, product_type = GetProductCategories(r.html)
    product_name = GetProductName(r.html)

    #variables for price per item
    price_per_item = GetPricePerItem(r.html)
    item = GetItem(r.html)

    #discount price check
    old_price = GetOldPrice(r.html)
    is_discounted = 1 if old_price is not None else 0

    #discount percentage
    discount_pct = GetDiscount(price_per_item, old_price, is_discounted)
    
    #discount period 
    discount_start, discount_end = GetDiscountPeriods(r.html)

    #variables for price per unit
    price_per_unit, unit = GetPricePerUnit(r.html)
    
    #variable for minimum amount
    minimum_amount = GetMinimumAmount(r.html)

    #extracting variables for product additional information from JavaScript or if parsing failed - trying to render it and re-scrape as a part of the same async session
    additional_info = GetProductInfo(r.html)
    
    if additional_info == None:
        try:
            r_rendered = await s.get(url, headers=header)
            await r_rendered.html.arender(sleep=5, timeout=20)
            additional_info = GetProductInfoRendered(r_rendered.html)
        except:
            additional_info = None

    #variables with product information
    origin_country = GetCountry(additional_info)
    brand = GetBrand(additional_info)
    producer = GetProducer(additional_info)
    amount = GetAmount(additional_info)
    alcohol = GetAlcohol(additional_info)

    #extraction timestamp
    extract_timestamp = datetime.datetime.now()

    #combining all results into a single list
    all_data = [
            snapshot_date, 
            product_code, 
            product_name, 
            product_category,
            product_subcategory, 
            product_type, 
            price_per_item, 
            item,
            old_price, 
            is_discounted,
            discount_pct, 
            discount_start, 
            discount_end,
            price_per_unit, 
            unit, 
            minimum_amount, 
            origin_country, 
            brand,
            producer, 
            amount, 
            alcohol, 
            extract_timestamp, 
            url
            ]
    
    return all_data
    

#control of steps of the async function
def product_detail_procedure(urls, header, snapshot_date, stop, step, sql_cursor, sql_conn):

    start = 0
    stop = stop
    step = step

    while start < len(urls):

        url_portion = urls[start:stop]
        results = s.run(*[lambda url=url: product_details(url, header, snapshot_date) for url in url_portion])

        for row in results:
            try:
                product_data = (row)
                sql_cursor.execute(product_insert_query, product_data)
                sql_conn.commit()
            except Exception as e:
                print(e)

        start += step
        stop += step

        time.sleep(3)

#------------------------------------------------------MAIN FUNCTION FOR THE SCRIPT------------------------------------------------------

#steps of the whole procedure
def Main():

    print(f'{datetime.datetime.now()} - Beginning scraping product information!')

    #headers for requests
    header = {'User-Agent': 'G. Suvalovs (glebs.suvalovs@gmail.com)'}

    #date of the process aka today's date
    snapshot_date = datetime.datetime.today().date()

    product_links = pd.read_sql(select_all_links, db_conn)
    url_list = product_links['product_link'].values.tolist()

    product_detail_procedure(url_list, header, snapshot_date, stop=30, step=30, sql_cursor=cursor, sql_conn=db_conn)

    cursor.close()
    db_conn.close()

    print(f'{datetime.datetime.now()} - Products are scraped!')