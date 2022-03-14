from GetProducts import *

#--------------------------------------------------SQL CONNECTIONS-------------------------------------------

server = '127.0.0.1:3306'
database = 'dbschema'
username = 'rimiuser'
password = '11pienas'

db_conn = pyodbc.connect('DRIVER={MySQL ODBC 8.0 ANSI Driver};Trusted_Connection=yes;SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = db_conn.cursor()

#--------------------------------------------------SQL QUERIES------------------------------------------------

empty_link_selector = """

declare @today date
set @today = ( select format( getdate(), 'yyyy-MM-dd' ) )
select product_link from rimi_data.product_data where snapshot_date = @today and origin_country is null
order by extract_timestamp asc;

"""

empty_link_truncator = """

declare @today date
set @today = ( select format( getdate(), 'yyyy-MM-dd' ) )
delete from rimi_data.product_data where snapshot_date = @today and origin_country is null;

"""

#steps of the whole procedure
def Main():

    print(f'{datetime.datetime.now()} - Beginning error handling!')

    #headers for requests
    header = {'User-Agent': 'G. Suvalovs (glebs.suvalovs@gmail.com)'}

    #date of the process aka today's date
    snapshot_date = datetime.datetime.today().date()

    #selecting the rows with empty values to rescrape them again
    product_links_with_errors = pd.read_sql(empty_link_selector, db_conn)
    url_list = product_links_with_errors['product_link'].values.tolist()

    #deleting the links with empty values not scraped without JS
    cursor.execute(empty_link_truncator)
    db_conn.commit()

    product_detail_procedure(url_list, header, snapshot_date, stop=5, step=5, sql_cursor=cursor, sql_conn=db_conn)

    cursor.close()
    db_conn.close()

    print(f'{datetime.datetime.now()} - Error handling procedure finished!')