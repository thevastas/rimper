from GetLinks import Main as GetLinksMain
from GetProducts import Main as GetProductsMain
from HandleErrors import Main as HandleErrorsMain
from time import sleep
import sys

def Main():

    GetLinksMain()

    sleep(5 * 60)

    GetProductsMain()

    sleep(5 * 60)

    HandleErrorsMain()

    sys.exit(0)

if __name__ == "__main__":
    Main()