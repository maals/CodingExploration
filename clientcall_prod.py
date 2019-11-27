#!/usr/bin/python
"""clientcall_prod.py: This python script is implemented to pull data from the production API and populate the TEST_SOURCE_ database."""
_author_ = "Malini Karthikeyan"
_version_= "1.0.1"

##import statements
import requests, json, os
import ConfigParser
import MySQLdb as mariadb
import datetime, time
import subprocess, ConfigParser
import pandas as pd
import sys
reload(sys)
sys.setdefaultencoding('utf8')

now = datetime.datetime.now()
start = time.time()
print str(start)
##read credentials from config file
thisfolder=os.path.dirname(sys.argv[0])
configfile = os.path.join(thisfolder, 'credentials.cfg')
#print(configfile)
config = ConfigParser.ConfigParser()
config.read(configfile)
email_value=config.get('LoginSection','email')
pwd_value=config.get('LoginSection','password')
logindata={'email':email_value,'password':pwd_value}
print(logindata)
signin_url=config.get('UrlSection','signin')


##Returns the max id of the table(logs)
def last_id(connection, table_name):
  cursor = connection.cursor()
  command_str = 'select max(id) from '+ table_name+';'
  try:
    cursor.execute(command_str)
    max_id = cursor.fetchone()
    print max_id
  except mariadb.Error as msg:
    connection.rollback()
    print msg
  connection.commit()
  cursor.close()
  return max_id[0]

###The purpose of this function is to create a sql file with set of insert comments
## from the given dict object and populate the tables.
def writeSql(mylist, table_name, mydict): 
   insertlist=[]
   print len(mylist)   
   for i in range(len(mylist)):
      insertdt=mylist[i]
      lst_1= mylist[i].values()
      lst_2= mylist[i].values()
      lst_3 = lst_1+lst_2
      tup_data=tuple(lst_3)
      placeholders=','.join(['%s']*len(insertdt))
      columns=','.join(insertdt.keys())
      sorted_column_values_list = []
      for k, v in insertdt.items():
           temp = k +' = values('+k+')'          
           sorted_column_values_list.append(temp)
      sorted_column_values_string = ', '.join(sorted_column_values_list)
      updatecols='=%s,'.join(insertdt.keys())
      updatecols=updatecols+'=%s'
      sql="insert into %s ( %s ) values (%s) on duplicate key update %s" % (table_name, columns, placeholders,updatecols)
      #print sql
      cursor = connection.cursor()
      try:
         cursor.execute(sql, tup_data)
         insertlist.append(i)
      except mariadb.Error as msg:
         connection.rollback()
         print msg
      connection.commit()
      cursor.close()

# Signin to the API and return the token.
def loginAPI():
 #signin_url=url_dict[]
 #logindata={'email':'arsene.ntiwa@smartdata-analytics.de','password':'12345678'}
 r= requests.post(signin_url,data=logindata)
 #print r.content
 # if the login is success then create a token and call the APIs
 if r.json().get('success')== True:
    token=r.json().get('data')
    #print token
    headers={'Authorization':'Bearer '+token}
    return headers
 else:
   print r.content
   exit()
    
headers = loginAPI()
##Connection string
connection = mariadb.connect(host=config.get('DatabaseSection','host'), user=config.get('DatabaseSection','user'), passwd=config.get('DatabaseSection','passwd'),db=config.get('DatabaseSection','db'), use_unicode=True, charset=config.get('DatabaseSection','charset'))

# call to the APIs
order=requests.get(config.get('UrlSection','ordersping'),headers=headers)
#print(str(datetime.datetime.now()))
#print order.json().get('orders') 
ord_data=order.json().get('data')
ord_lst=ord_data['orders']
#print type(ord_data)
print len(ord_lst)
if ord_lst:
##get lists of order_id, client_id and user_id from the order_list
    id_lst = [d['id'] for d in ord_lst]
    id_lst = [i for i in id_lst if i is not None]
    usrid_lst = [d['user_id'] for d in ord_lst]
    usrid_lst = [i for i in usrid_lst if i is not None]
    cliid_lst = [d['client_id'] for d in ord_lst]
    cliid_lst = [i for i in cliid_lst if i is not None]
    writeSql(ord_lst,'orders',ord_data)
    #run_sql_file(orders_list, connection)
    #declare list and dict for all api calls
    fi_lst = []
    acc_lst =[]
    usr_lst = []
    add_lst = []
    cli_lst = []
    fi_dict = {}
    acc_dict = {}
    usr_dict = {}
    add_dict = {}
    cli_dict = {}
    logs_lst=[]
    logs_dict={}
    print id_lst
    print usrid_lst
    print cliid_lst
    print 'len of clients, address, users, accounts, files'
    headers = loginAPI()
    for item in id_lst:
      fi=requests.get(config.get('UrlSection','filesorder')%(item),headers=headers)
      acc=requests.get(config.get('UrlSection','accountsorder')%(item),headers=headers)
      #print acc.content
      if(fi.json().get('success')==True):
        fi_dict=fi.json().get('data')
        fi_lst=fi_dict['files']
      if(acc.json().get('success')==True):
        acc_dict=acc.json().get('data')
        acc_lst=acc_dict['accounts']
    if len(fi_lst)>0:
      writeSql(fi_lst,'files',fi_dict)
      #run_sql_file(file_lst, connection)
    if len(acc_lst)>0:
      writeSql(acc_lst,'accounts',acc_dict)
      #run_sql_file(accounts_list, connection)
    headers = loginAPI()
    for item in cliid_lst:
      cli=requests.get(config.get('UrlSection','clientsid')%(item),headers=headers)
      add1=requests.get(config.get('UrlSection','addressclients')%(item),headers=headers)
      if(cli.json().get('success')==True):
        cli_dict=cli.json().get('data')
        cli_lst=cli_dict['clients']
      if(add1.json().get('success')==True):
        add_dict=add1.json().get('data')
        add_lst=add_dict['addresses']
    print 'len of address after cli for loop'
    print len(add_lst)
    headers = loginAPI()   
    for item in usrid_lst:
      usr=requests.get(config.get('UrlSection','usersid')%(item),headers=headers)
      add2=requests.get(config.get('UrlSection','addressusers')%(item),headers=headers)
      if(usr.json().get('success')==True):
        usr_dict=usr.json().get('data')
        usr_lst=usr_dict['users']
      if(add2.json().get('success')==True):
        add_dict=add2.json().get('data')
        add_lst=add_dict['addresses'] 
    if len(cli_lst)>0:
      writeSql(cli_lst,'clients',cli_dict)
      #run_sql_file(client_list, connection)
    if len(usr_lst)>0:
      writeSql(usr_lst,'users',usr_dict)
      #run_sql_file(user_list, connection)
    if len(add_lst)>0:
      writeSql(add_lst,'addresses',add_dict)
      #run_sql_file(address_list, connection)
    print 'len of address after cli for loop'
    print len(add_lst)
    ##logs coding starts from here..
    max_id = last_id(connection, 'logs')
    condition = True
    offset = 0
    headers = loginAPI()
    while condition:
      logs=requests.get(config.get('UrlSection','logs')%(offset),headers=headers)
      logs=logs.json().get('data')
      logs_lst=logs['logs']
      min_id = min(d['id'] for d in logs_lst)
      print min_id
      #print [d['id'] for d in logs_lst]
      print 'max_id:'
      print max_id
      latest_logs_lst= [d for d in logs_lst if d['id']>max_id]
      if len(latest_logs_lst)>0:
        df = pd.DataFrame(latest_logs_lst)
        #print df
        try:
          df.to_sql(con=connection, name='logs', if_exists='append', flavor='mysql', index=False)
        except IntegrityError as e:
          print e
      offset = offset + 200
      if min_id <= max_id:
        condition = False
      ##Calling R script
    print 'END OF PYTHON...CALLING R SCRIPT..'
    rscriptname=config.get('PathSection','rscriptname')
    retcode=subprocess.call(rscriptname, shell=True) 
    print retcode 
    
   ##read logs file to post
    logs_lst=[]
    fname_lst=[]
    index=0
    print('after Rscript')
    dirName=config.get('PathSection','dirname')
    for file in os.listdir(dirName):
      if file.endswith('.json'):
        fname_lst.append(os.path.join(dirName, file))
        #print fname_lst
        payload = json.loads(open(fname_lst[index]).read())
        logs_lst.append(payload)
        #print logs_lst
        index=index+1
        
     #print logs_list
    print len(logs_lst)
    if len(logs_lst)>0:
       r= requests.post(signin_url,data=logindata)
       #print r.content
       if r.json().get('success')== True:
           token=r.json().get('data')
           headers={'Authorization':'Bearer '+token}
           print('before iogpost')
           for i, payload in enumerate(logs_lst):
             print fname_lst[i]
             print logs_lst[i]
             res= requests.post(config.get('UrlSection','logspost'), headers=headers, json=payload)
             print res.content
             if res.json().get('success')==True:
               os.remove(fname_lst[i])
    
    connection.close()
    end = time.time()
    print "Time elapsed to run the script:"
    print str((end - start)*1000) + ' ms'
else:
    print('Order ping is empty:prod')
        
   



    
