#---------------------------------------------------LIBRARIES----------------------------------------------

import pyodbc
import datetime
from requests_html import HTMLSession

#--------------------------------------------------SQL CONNECTIONS-------------------------------------------

db_conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=DESKTOP-LP9CHI3;'
                      'Database=rimi_data;'
                      'Trusted_Connection=yes;')
cursor = db_conn.cursor()

#--------------------------------------------------SQL QUERIES------------------------------------------------

link_drop_query = """

drop table if exists rimi_data.e_rimi.product_links;

"""

link_table_create = """

create table rimi_data.e_rimi.product_links (

    product_category varchar(max),
    product_link varchar(max),
    scrape_timestamp datetime
)

"""

link_insert_query = """

insert into rimi_data.e_rimi.product_links ( product_category, product_link, scrape_timestamp )
values (?, ?, ?);

"""

#----------------------------------------------------------PRODUCT CATEGORY LINK LISTS----------------------------------

product_category_links = [
    'https://www.rimi.lv/e-veikals/en/products/fruit-and-vegetables/c/SH-2',
    'https://www.rimi.lv/e-veikals/en/products/vegan-and-vegetarian-/c/SH-16',
    'https://www.rimi.lv/e-veikals/en/products/meat-fish-and-culinary/c/SH-6',
    'https://www.rimi.lv/e-veikals/en/products/dairy-and-eggs/c/SH-11',
    'https://www.rimi.lv/e-veikals/en/products/bread-and-pastries/c/SH-7',
    'https://www.rimi.lv/e-veikals/en/products/frozen-food/c/SH-12',
    'https://www.rimi.lv/e-veikals/en/products/food-cupboard/c/SH-4',
    'https://www.rimi.lv/e-veikals/en/products/sweets-and-snacks/c/SH-13',
    'https://www.rimi.lv/e-veikals/en/products/drinks/c/SH-5',
    'https://www.rimi.lv/e-veikals/en/products/alcoholic-beverages/c/SH-1',
    'https://www.rimi.lv/e-veikals/en/products/beauty-and-hygiene/c/SH-14',
    'https://www.rimi.lv/e-veikals/en/products/babies-and-children/c/SH-15',
    'https://www.rimi.lv/e-veikals/en/products/detergents-and-cleaning-supplies/c/SH-10',
    'https://www.rimi.lv/e-veikals/en/products/pets/c/SH-8',
    'https://www.rimi.lv/e-veikals/en/products/house-garden-and-leisure/c/SH-3'
]

#------------------------------------------------------------PRODUCT CATEGORY SCRAPE FUNCTION---------------------------------------------------

def product_link_procedure(link_list, header):

    #initializing HTML session
    non_assync_session = HTMLSession()

    for i in link_list:

        #splitting the link string and extractig product code
        splitted_string = i.split('/')
        category_code = splitted_string[-1]

        #initial page number
        page_number = 1
        
        #initial/default list_length
        nr_items = 80

        while nr_items > 0:

            query_url_part = f'?page={page_number}&pageSize=80&query=%3Arelevance%3AallCategories%3A{category_code}%3AassortmentStatus%3AinAssortment'
            url = i+query_url_part

            #extracting content from the url
            try:
                scrape_content = non_assync_session.get(url, headers=header)
            except Exception as e:
                print(e)
                pass

            #accesing product information on Rimi website and checking how many products there are
            product_information = scrape_content.html.find('li.product-grid__item')
            list_length = len(product_information)

            #updating variables for the control flow of the process with page number and length of product list for the while loop
            nr_items = list_length
            page_number += 1

            #controlling the flow depending on how many items there are on the page
            if nr_items > 0:

                #accessing the product links on each page
                for item in product_information:
                    result = item.find('div')
                    for item in result:
                        result = item.find('a')
                        for item in result:
                            product_link = 'https://www.rimi.lv'+item.attrs['href']

                            #SQL server inserts
                            try:
                                items = (url, product_link, datetime.datetime.now())
                                cursor.execute(link_insert_query, items)
                                db_conn.commit()
                            except Exception as e:
                                print(e)
                                pass

            #ending the portion of the while loop to go on to next element                   
            else:
                pass

#------------------------------------------------------MAIN FUNCTION FOR THE SCRIPT------------------------------------------------------

#steps of the whole procedure
def Main():

    print(f'{datetime.datetime.now()} - Beginning scraping product links!')

    #headers for requests
    header = {'User-Agent': 'G. Suvalovs (glebs.suvalovs@gmail.com)'}

    #deleting the old version of the link table on SQL server
    cursor.execute(link_drop_query)
    db_conn.commit()

    #creating the table again on SQL server
    cursor.execute(link_table_create)
    db_conn.commit()

    product_link_procedure(product_category_links, header)

    cursor.close()
    db_conn.close()
    
    print(f'{datetime.datetime.now()} - Links are saved!')