import smtplib
import pyodbc
import pandas as pd
import datetime
from email.message import EmailMessage

server = '127.0.0.1:3306'
database = 'dbschema'
username = 'rimiuser'
password = '11pienas'

db_conn = pyodbc.connect('DRIVER={MySQL ODBC 8.0 ANSI Driver};Trusted_Connection=yes;SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = db_conn.cursor()

def Main():

    #importing data to be sent as emails
    import_check = pd.read_sql('set nocount on; exec e_rimi.last_import_check', db_conn)
    quality_check = pd.read_sql('set nocount on; exec e_rimi.quality_check', db_conn).round(2)

    #reading login credentials
    f = open("credentials.txt", encoding = 'utf-8')
    contents = f.read().split(', ')
    login = contents[0]
    password = contents[1]

    #formatting the data and message tittles
    data_list = [import_check.to_html(), quality_check[:7].T.to_html()]
    message_titles = [f'RIMI data import report for {datetime.datetime.now().date()}', f'RIMI data quality report for {datetime.datetime.now().date()}']

    #sending the messages
    for d, t in zip(data_list, message_titles):

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(login, password)

            try:
                msg = EmailMessage()
                msg.set_content('The report was empty. Please check!')
                msg.add_alternative(f'{d}', subtype='html')
                msg['Subject'] = t
                msg['From'] = 'glebs.suvalovs@gmail.com'
                msg['To'] = 'gleb.shuvalov@gmail.com'
                smtp.send_message(msg)
                
            except Exception as e:
                print(e)
                pass

    #closing all connections
    f.close()
    cursor.close()
    db_conn.close()