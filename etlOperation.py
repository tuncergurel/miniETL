import dataToPostgres as posgre
import dataFromMssql as tsql
import datetime

####### MSSQL ###################
def GetOrders_MSSQL(): 
    getDataFromMssql = tsql.GetOrders()
    return getDataFromMssql

def GetOrdersDetails_MSSQL(): 
    getDataFromMssql = tsql.GetOrdersDetails()
    return getDataFromMssql



####### POSTGRESQL ###################
def GetOrders_PostgreSql():
    datas = GetOrders_MSSQL()
    posgre.GetOrders(datas) 

def GetOrdersDetails_PostgreSql():
    datas = GetOrdersDetails_MSSQL()
    posgre.GetOrdersDetails(datas) 



####### MAIN ###################
def main():
    start = datetime.datetime.now().time().strftime('%H:%M:%S')
    print(start)
    GetOrders_PostgreSql()
    GetOrdersDetails_PostgreSql()
    done = datetime.datetime.now().time().strftime('%H:%M:%S')
    print(done)

    elapsed = (datetime.datetime.strptime(done,'%H:%M:%S')-datetime.datetime.strptime(start,'%H:%M:%S'))
    print("ETL Elapsed Time: {}".format(elapsed))
    
if __name__ == '__main__':
    main()
