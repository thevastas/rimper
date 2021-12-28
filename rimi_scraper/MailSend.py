import smtplib
import pyodbc
import pandas as pd
import datetime
from email.message import EmailMessage

db_conn = pyodbc.connect('Driver={ODBC Driver 17 for SQL Server};'
                      'Server=DESKTOP-LP9CHI3;'
                      'Database=rimi_data;'
                      'Trusted_Connection=yes;')
cursor = db_conn.cursor()

def Main():

    df = pd.read_sql('set nocount on; exec e_rimi.last_import_check', db_conn)
    message = f'{df.to_html()}'

    f = open("credentials.txt", encoding = 'utf-8')
    contents = f.read().split(', ')
    login = contents[0]
    password = contents[1]

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
        smtp.login(login, password)
        try:
            msg = EmailMessage()
            msg.set_content('The report was empty. Please check!')
            msg.add_alternative(message, subtype='html')
            msg['Subject'] = f'RIMI data extraction report for {datetime.datetime.now().date()}'
            msg['From'] = 'glebs.suvalovs@gmail.com'
            msg['To'] = 'gleb.shuvalov@gmail.com'
            smtp.send_message(msg)
        except:
            pass

    f.close()