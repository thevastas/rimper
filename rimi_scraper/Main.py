from GetLinks import Main as GetLinksMain
from GetProducts import Main as GetProductsMain
from HandleErrors import Main as HandleErrorsMain
from MailSend import Main as MailSendMain
from time import sleep
import sys

def Main():

    GetLinksMain()

    sleep(2 * 60)

    GetProductsMain()

    sleep(2 * 60)

    HandleErrorsMain()

    sleep(10)

    MailSendMain()

    sys.exit(0)

if __name__ == "__main__":
    Main()