import psycopg2
from psycopg2 import Error

def postgresql():
    try:
        connection = psycopg2.connect(user="postgres",
                                      password="pass",
                                      host="host",
                                      port="port",
                                      database="AdventureWorks2019")

        connection

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

    return connection

def GetOrders(datas): 
    psql = postgresql()
    cur = psql.cursor()
    allOrders = datas

    
    try:
        for i in allOrders:
            salesorderid = i['SalesOrderID']
            revisionnumber = i['RevisionNumber']
            orderdate = i['OrderDate']
            duedate = i['DueDate']
            shipdate = i['ShipDate']
            status = i['Status']
            onlineorderflag = 0 if i['OnlineOrderFlag'] == False else 1
            orderaccountnumber = i['AccountNumber']
            customerid = i['CustomerID']
            salespersonid = i['SalesPersonID']
            territoryid = i['TerritoryID']
            billtoaddressid = i['BillToAddressID']
            shiptoaddressid = i['ShipToAddressID']
            shipmethodid = i['ShipMethodID']
            creditcardid = i['CreditCardID']
            currencyrateid = i['CurrencyRateID']
            subtotal = i['SubTotal']
            taxamt = i['TaxAmt']
            freight = i['Freight']
            totaldue = i['TotalDue']
            modifieddate = i['ModifiedDate']
            personid = i['PersonID']
            storeid = i['StoreID']
            accountnumber = i['AccountNumber']
            businessentityid = i['BusinessEntityID']
            persontype = i['PersonType']
            title = i['Title']
            firstname = i['FirstName']
            middlename = i['MiddleName']
            lastname = i['LastName']
            suffix = i['Suffix']
            emailpromotion = i['EmailPromotion']

            cur.execute('insert into getOrders(SalesOrderID,RevisionNumber,OrderDate,DueDate,ShipDate,Status,OnlineOrderFlag,'
                     'OrderAccountNumber,CustomerID,SalesPersonID,TerritoryID,BillToAddressID,ShipToAddressID,ShipMethodID,'
                     'CreditCardID,CurrencyRateID,SubTotal,TaxAmt,Freight,TotalDue,ModifiedDate,PersonID,StoreID,CustomerAccountNumber,'
                     'BusinessEntityID,PersonType,Title,FirstName,MiddleName,LastName,Suffix,EmailPromotion) '
                     'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                         (salesorderid,revisionnumber,orderdate,duedate,shipdate,status,onlineorderflag,orderaccountnumber,customerid,
                          salespersonid,territoryid,billtoaddressid,shiptoaddressid,shipmethodid,creditcardid,currencyrateid,
                          subtotal,taxamt,freight,totaldue,modifieddate,personid,storeid,accountnumber,businessentityid,
                          persontype,title,firstname,middlename,lastname,suffix,emailpromotion))
            psql.commit()
    except psycopg2.DatabaseError as msg:
        psql.rollback()
        print(msg)
    psql.close()


def GetOrdersDetails(datas): 
    psql = postgresql()
    cur = psql.cursor()
    allOrders = datas

    
    try:
        for i in allOrders:
            salesorderid = i['SalesOrderID']
            purchaseordernumber = i['PurchaseOrderNumber']
            orderdate = i['OrderDate']
            customerid = i['CustomerID']
            personid = i['PersonID']
            productid = i['ProductID']
            productname = i['ProductName']
            productnumber = i['ProductNumber']
            color = i['Color']
            safetystocklevel = i['SafetyStockLevel']
            standardcost = i['StandardCost']
            listprice = i['ListPrice']
            size = i['Size']
            sizeunitmeasurecode = i['SizeUnitMeasureCode']
            weightunitmeasurecode = i['WeightUnitMeasureCode']
            weight = i['Weight']
            productsubcategoryid = i['ProductSubcategoryID']
            productmodelid = i['ProductModelID']
            orderqty = i['OrderQty']
            unitprice = i['UnitPrice']
            unitpricediscount = i['UnitPriceDiscount']
            linetotal = i['LineTotal']
            businessentityid = i['BusinessEntityID']
            accountnumber = i['AccountNumber']
            vendorname = i['VendorName']
            activeflag = 0 if i['ActiveFlag'] == False else 1

            cur.execute('insert into getordersdetails(SalesOrderID,PurchaseOrderNumber,OrderDate,CustomerID,PersonID,'
                        'ProductID,ProductName,ProductNumber,Color,SafetyStockLevel,StandardCost,ListPrice,Size,'
                        'SizeUnitMeasureCode,WeightUnitMeasureCode,Weight,ProductSubcategoryID,ProductModelID,OrderQty,'
                        'UnitPrice,UnitPriceDiscount,LineTotal,BusinessEntityID,AccountNumber,VendorName,ActiveFlag) '
                     'values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',
                         (salesorderid,purchaseordernumber,orderdate,customerid,personid,productid,productname,
                          productnumber,color,safetystocklevel,standardcost,listprice,size,sizeunitmeasurecode,
                          weightunitmeasurecode,weight,productsubcategoryid,productmodelid,orderqty,unitprice,
                          unitpricediscount,linetotal,businessentityid,accountnumber,vendorname,activeflag))
            psql.commit()
       
    except psycopg2.DatabaseError as msg:
        psql.rollback()
        print(msg)
    psql.close()