#!/usr/bin/env python3
# coding: utf-8
#
#
# MoneyManager export: a tool to export data from the Money Manager Expense & budget app in an useful way (for me)
#
#

import argparse
import sqlite3
from datetime import datetime, date
from calendar import monthrange
import sys

# Program arguments
parser = argparse.ArgumentParser(prog='mmgrexport.py',
                    description='Export Money Manager transactions in a suitable format for later analysis',
                    add_help=True)

fileGroup = parser.add_argument_group('File', 'Backup file to process')
fileGroup.add_argument('sqlite3_file',
                    help='The exported backup file from Money Manager'
                    )

dateGroup = parser.add_argument_group('Date selection', 'Determine date range to process')
dateGroup.add_argument('-s', '--start',
                    metavar='startDate',
                    help='Start date in format "YYYY-MM-DD". If not provided, the 1st day of last month is used',
                    )

dateGroup.add_argument('-e', '--end',
                    metavar='endDate',
                    help='End date in format "YYYY-MM-DD". If not provided, the last day of last month is used')

dateGroup.add_argument('-m', '--month',
                    metavar='month',
                    help='Process full `month\' from current year. Accepted values are numeric,' +
                    ' or `Jan\'/`January\', or `Ene\'/`Enero\', etc; case insensitive. Takes precedence over '+
                    ' the other date options' )

parser.add_argument('-d', '--debug',
                    help='Increase program debug messages. Can be specified multiple times ("-dd", "-ddd", etc)',
                    action='count', default=0)

class MoneyManagerQuery:
    """Extract useful data from a Money Manager database"""

    def __init__(self, dbFilePath, debugLevel):
        # Init class attributes
        self.dbName = dbFilePath
        self.dbCon = sqlite3.connect(self.dbName)
        self.queryStatement = None
        self.queryResult = []
        self.startDate = None
        self.endDate = None
        self.totalAmount = 0
        self.debugLevel = debugLevel

    def __monthStrToNum(self, monthStr):
        """Transforms a string representing the month into a number in the range 1-12"""
        return {
            'jan' : 1,
            'january' : 1,
            'ene': 1,
            'enero': 1,
            'feb': 2,
            'february': 2,
            'febrero': 2,
            'mar': 3,
            'march': 3,
            'marzo': 3,
            'apr': 4,
            'april': 4,
            'abr': 4,
            'abril': 4,
            'may': 5,
            'mayo': 5,
            'jun': 6,
            'june': 6,
            'junio': 6,
            'jul': 7,
            'july': 7,
            'julio': 7,
            'aug': 8,
            'august': 8,
            'ago': 8,
            'agosto': 8,
            'sep': 9,
            'september': 9,
            'septiembre': 9,
            'oct': 10,
            'october': 10,
            'octubre': 10,
            'nov': 11,
            'november': 11,
            'noviembre': 11,
            'dec': 12,
            'december': 12,
            'dic': 12,
            'diciembre': 12,
        }.get(monthStr.lower(), 0) # 0 is the default value if not found

    def __parseMonth(self, monthStr):
        """Transforms a string into a month in the interval 1-12.

        Input string can be a number such as "1", "02", or a month such
        as "Jan", "January, "Ene", "Enero", etc.
        Returns 0 if the month could not be parsed
                1-12 if the month could be parsed (January: 1)
        """
        month = 0

        # First, try to obtain a numeric month
        try:
            # cast to int
            month = int(monthStr)
            if month < 1 or month > 12:
                # Fallback to current month
                month = datetime.now().month
        except:
            pass

        # Second, try to obtain the month from a string
        if  month == 0:
            month = self.__monthStrToNum(str(monthStr))

        return month

    def getStartDate(self):
        if self.startDate == None:
            # Use last month if no date requested.
            if datetime.now().month == 1:
                self.startDate = date(datetime.now().year-1, 12, 1)
            else:
                self.startDate = date(datetime.now().year, datetime.now().month-1, 1)

        return self.startDate

    def setStartDate(self, startDate):
        self.startDate = startDate
        return self

    def getEndDate(self):
        if self.endDate == None:
            numberOfDaysInMonth = monthrange(self.getStartDate().year, self.getStartDate().month)[1]
            self.endDate = date(self.getStartDate().year, self.getStartDate().month, numberOfDaysInMonth)

        return self.endDate

    def setEndDate(self, endDate):
        self.endDate = endDate
        return self

    def setMonth(self, monthStr):
        """Set startDate and endDate to process the full `monthStr' of current year"""
        month = self.__parseMonth(monthStr)

        # If month != 0 it means we could understand it: create appropriate strings for start and end dates.
        # Otherwise, remove start and end date (just in case user also put them in command line): we will
        # automatically use last month for dates.
        if month != 0:
            endDay = monthrange(datetime.now().year, month)[1]
            self.setStartDate(date(datetime.now().year, month, 1))
            self.setEndDate(date(datetime.now().year, month, endDay))
        else:
            # month == 0
            self.setStartDate(None)
            self.setEndDate(None)


    def getQueryStatement(self):
        if self.queryStatement == None:
            localStartDate = str(self.getStartDate())
            localEndDate = str(self.getEndDate())

            if self.debugLevel >= 1:
                print ("Date range: " + localStartDate, "-", localEndDate)
            # TODO: sanitize input
            self.queryStatement = "SELECT z.zdate, z.ztxdatestr, c.zname, z.zcontent, z.zamount, a.znicname ";
            self.queryStatement += "FROM ZASSET a, ZCATEGORY c, ZINOUTCOME z ";
            self.queryStatement += "WHERE z.ztxdatestr ";
            self.queryStatement += "BETWEEN \""+localStartDate+"\" AND \""+localEndDate+"\" ";
            self.queryStatement += "AND z.zisdel = 0 ";                  # zisdel flags deleted entries
            self.queryStatement += "AND z.zdo_type = 1 ";                # Type 1 is "expenses"
            self.queryStatement += "AND z.ZASSETUID = a.ZUID "           # Join asset (pay method)
            self.queryStatement += "AND z.ZCATEGORYUID = c.ZUID "        # Join Category
            self.queryStatement += "ORDER BY z.zdate ASC";
        return self.queryStatement

    def processDate(self, strDate):
        (year, month, day) = strDate.split("-")
        dt = date(int(year), int(month), int(day))
        return dt.strftime("%d/%m/%Y")

    def processCategory(self, category):
        if category.find("/") >= 0:
            # This category is actually "Category/Sub-category"
            retVal = (category.split("/"))[1]
        else:
            retVal= category

        return retVal
        
    def processAmount(self, amount):
        # Transform "x.y" into "x,y". Grab only 2 elems after split(), just in case (I don't trust DB data)
        (amt_integer, amt_cents) = str(amount).split(".")[:2]

        # Put a nice ending "0" in the cents part if needed
        if len(str(amt_cents)) < 2:
            amt_cents = str(amt_cents) + "0"
        return ",".join([amt_integer, amt_cents])


    def processPaymentMethod(self, pm):
        retVal = ""
        pm = pm.strip()

        if self.debugLevel >= 2:
            print("pay mehtod: '" + pm + "'")
        if pm == "Tickets":
            retVal = "Ti"
        elif pm == u"Transferencia":
            retVal = "T"
        elif pm == "Efectivo":
            retVal = "E"
        elif pm == u"T. Débito":
            retVal = "TD"
        elif pm == "T. Crédito":
            retVal = "TC"
        elif pm == "PayPal":
            retVal = "P"
        else:
            retVal = "INVALID"

        return retVal

    def processName(self, tr_name):
        return tr_name

    def getResult(self):
        """Returns list with all the selected transactions, already formatted

        Each element in the list is a list [date, category, name, amount, pay_method]
        """
        result = []
        cursor = self.dbCon.cursor()

        for transaction in cursor.execute(self.getQueryStatement()):
            if self.debugLevel >= 1:
                print (transaction)
#Zutime: timestamp (unix format con millis) of record creation
#Zdate : timestamp (cocoa format) del gasto. https://www.thecodeship.com/general/converting-cocoa-unix-timestamp/
            row = []
            (tr_cocoa_timestamp, tr_date, tr_category, tr_name, tr_amount, tr_pay_method) = transaction

            # tr_cocoa_timestamp: skip. Left here as a reminder.
            
            # tr_date:
            row.append(self.processDate(tr_date))

            # tr_category:
            row.append(self.processCategory(tr_category))

            # tr_name
            row.append(self.processName(tr_name))

            # tr_amount:
            row.append(self.processAmount(tr_amount))

            # tr_pay_method:
            row.append(self.processPaymentMethod(tr_pay_method))

            # Current row is ready
            result.append(row)

            # Accumulate total amount
            self.totalAmount += tr_amount

        return result

    def toString(self):
        retVal = u"fecha;categoría;comentario;importe;forma pago\n";
        for transaction in self.getResult():
            for i in range(4):
                retVal += transaction[i] + u";"

            retVal += transaction[4] + u"\n"

        retVal += "Total: " + str(round(self.totalAmount, 2));
        return retVal

    def toCSV(self):
        return self.toString()

if __name__ == "__main__":
    args = parser.parse_args()

    mmquery = MoneyManagerQuery(args.sqlite3_file, args.debug)
    # TODO: sanitize input
    if args.month != None:
        mmquery.setMonth(args.month)
    else:
        mmquery.setStartDate(args.start)
        mmquery.setEndDate(args.end)

    result = mmquery.toCSV()
    print (result)
