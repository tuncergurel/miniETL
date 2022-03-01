import pyodbc
def sqlConnect():
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=server;'
                          'Database=AdventureWorks2019;'
                          'Trusted_Connection=yes;')

    cursor = conn.cursor()
    return cursor

def GetOrders(): 
    cur = sqlConnect()
    query = """ SET NOCOUNT ON;            
            EXEC GetOrdersSP"""
    cur.execute(query)
    record = cur.fetchall()
    list_record = []
    for i in record:
        datas =    {'SalesOrderID': i[0],
                    'RevisionNumber': i[1],
                    'OrderDate': i[2],
                    'DueDate': i[3],
                    'ShipDate': i[4],
                    'Status': i[5],
                    'OnlineOrderFlag': i[6],
                    'AccountNumber': i[7],
                    'CustomerID': i[8],
                    'SalesPersonID': i[9],
                    'TerritoryID': i[10],
                    'BillToAddressID': i[11],
                    'ShipToAddressID': i[12],
                    'ShipMethodID': i[13],
                    'CreditCardID': i[14],
                    'CurrencyRateID': i[15],
                    'SubTotal': i[16],
                    'TaxAmt': i[17],
                    'Freight': i[18],
                    'TotalDue': i[19],
                    'ModifiedDate': i[20],
                    'PersonID': i[21],
                    'StoreID': i[22],
                    'AccountNumber': i[23],
                    'BusinessEntityID': i[24],
                    'PersonType': i[25],
                    'Title': i[26],
                    'FirstName': i[27],
                    'MiddleName': i[28],
                    'LastName': i[29],
                    'Suffix': i[30],
                    'EmailPromotion': i[31]
                 }
        list_record.append(datas)
    return list_record

def GetOrdersDetails(): 
    cur = sqlConnect()
    query = """ SET NOCOUNT ON;            
            EXEC GetOrdersDetailsSP"""
    cur.execute(query)
    record = cur.fetchall()
    list_record = []
    for i in record:
        datas =    {'SalesOrderID':             i[0],
                    'PurchaseOrderNumber':      i[1],
                    'OrderDate':                i[2],
                    'CustomerID':               i[3],
                    'PersonID':                 i[4],
                    'ProductID':                i[5],
                    'ProductName':              i[6],
                    'ProductNumber':            i[7],
                    'Color':                    i[8],
                    'SafetyStockLevel':         i[9],
                    'StandardCost':             i[10],
                    'ListPrice':                i[11],
                    'Size':                     i[12],
                    'SizeUnitMeasureCode':      i[13],
                    'WeightUnitMeasureCode':    i[14],
                    'Weight':                   i[15],
                    'ProductSubcategoryID':     i[16],
                    'ProductModelID':           i[17],
                    'OrderQty':                 i[18],
                    'UnitPrice':                i[19],
                    'UnitPriceDiscount':        i[20],
                    'LineTotal':                i[21],
                    'BusinessEntityID':         i[22],
                    'AccountNumber':            i[23],
                    'VendorName':               i[24],
                    'ActiveFlag':               i[25],
                 }
        list_record.append(datas)
    return list_record
