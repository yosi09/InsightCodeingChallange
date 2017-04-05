import numpy as np
import pandas as pd
import time  #for tic - toc
import datetime
import sys




############################################################
#Feature 1: count host
def countHostsBis(f1,h):
    tmp=h.loc[:,0].value_counts()
    tmp.name='Count'
    if f1.empty:
        return tmp
    else:
        return f1.add(tmp,fill_value=0)


    
############################################################
#Feature 2: Resources
def BandWidth (f1, h):
    #tmp=h[[5,7]]
    tmp=pd.DataFrame({'Domain':h[5].str.partition(' ')[2].str.partition(' ')[0]})
    tmp['BandWidth']=pd.to_numeric(h.loc[:,7],errors='coerce')
    #tmp.loc[:,5]=h[5].str.partition(' ')[2].str.partition(' ')[0]
    #tmp.loc[:,7]=pd.to_numeric(tmp.loc[:,7],errors='coerce')
    if f1.empty:
        return tmp.fillna(0).groupby('Domain')['BandWidth'].sum()
    else:
        tmp=tmp.fillna(0).groupby('Domain')['BandWidth'].sum()
        return f1.add(tmp,fill_value=0)
  
############################################################
#Feature 3: busiest hours
def BusiestHoursRolling(f1, h,last60min):
    dt=h[3].str.strip('[')
    dt=pd.to_datetime(dt, format="%d/%b/%Y:%X")
    dt=dt.to_frame()
    dt.set_index(pd.DatetimeIndex(dt[3]),inplace=True)
    dt.loc[:,3]=1
    
    dt=dt.groupby(pd.Grouper(freq='1s')).sum()
    dt=dt.fillna(0)
    
    if dt.index[-1]-dt.index[0]<pd.Timedelta('60min'):
        last60min=dt
    else:
        if f1.empty:
            f1=dt[3].rolling(3600).sum().shift(-3599).to_frame()
        else:
            
            f1=pd.concat([f1,dt[3].rolling(3600).sum().shift(-3599).to_frame()])
        last60min=dt[dt.index>dt.index[-1]-pd.Timedelta('60min')+1]
        f1['idx']=f1.index
        f1=f1.sort_values(by=[3,'idx'],ascending=[False,True])
        f1.drop('idx',axis=1,inplace=True)
    return f1[:10], last60min
        


############################################################
#Feature 3: busiest hours
def BusiestHours(f1, h,last60min):
    dt=h[3].str.strip('[')
    dt=pd.to_datetime(dt, format="%d/%b/%Y:%X")
    dt=dt.to_frame()
    dt.set_index(pd.DatetimeIndex(dt[3]),inplace=True)
    dt.loc[:,3]=1    
    #if f1.empty:
        
    dt=dt.groupby(pd.Grouper(freq='1s')).sum()
    dt=dt.add(last60min,fill_value=0)
    
    for ind, row in dt.iterrows():
        if ind<dt.index[-1]-pd.Timedelta('60min'):
            dt[3][ind]=dt[3][ind:ind+pd.Timedelta('59m59s')].sum()
        else:
            break

    last60min=dt[ind:]
    dt=dt[dt.index<ind]
    f1=pd.concat([f1,dt])
    f1['idx']=f1.index
    f1=f1.sort_values(by=[3,'idx'],ascending=[False,True])
    f1.drop('idx',axis=1,inplace=True)
   # print(last60min)
   # print(f1)
    return f1[:10], last60min           
                

############################################################
#Feature 5: busiest hours No Overlap
def BusiestHoursBis(f1, h):
    dt=h[3].str.strip('[')
    dt=pd.to_datetime(dt, format="%d/%b/%Y:%X")
    dt=dt.to_frame()
    dt.set_index(pd.DatetimeIndex(dt[3]),inplace=True)
    dt.loc[:,3]=1    
    if f1.empty:
        return dt.groupby(pd.Grouper(freq='60T')).sum()
    else:
        dt=dt.groupby(pd.Grouper(freq='60T')).sum()
        return f1.add(dt,fill_value=0)

############################################################
#Feature 4: Blocked
def BlockedRequests(f1,h,blocked20, failed):
    #groupHosts=h.groupby(0)
    dt=h[3].str.strip('[')
    dt=pd.to_datetime(dt, format="%d/%b/%Y:%X")    
    dt=dt.to_frame()
    dt['Reply']=h[6]
    dt['Host']=h[0]
    dt.reset_index(inplace=True)
    dt.set_index('Host', inplace=True)
    dt.rename(columns={3:'Date', 'index':'h_ind'}, inplace=True)
    runtime=dt.Date[0]

    failedGroup=failed['Host'].value_counts()
    for ind, row in dt.iterrows():
        if ind in blocked20.index:
            #h.loc[row.h_ind,:].to_csv(BLOCKED_FILE,sep=' ', mode='a', header=None, index=False)
            f1=f1.append(h.loc[row.h_ind,:],ignore_index=True)
        else:
            if row.Reply==401:
                if ind in failedGroup.index:   # if the host is already failed
                    if failedGroup[ind]==2:  # if it is the second
                        blocked20=blocked20.append(pd.DataFrame({'Time':row.Date+pd.Timedelta('5M')}, index=[ind]))
                        failed=failed[failed.Host!=ind]
                        failedGroup=failedGroup[failedGroup.index!=ind]
                #        print(str(row.Date)+' Two timed in ' + ind)
                    #    print(failedGroup)
                    else:
                        failed=failed.append(pd.DataFrame({'Host':ind},index=[row.Date+pd.Timedelta('20s')]))
                        failedGroup[ind]+=1
               #         print(str(row.Date)+' Already in ' + ind)
                   #     print(failedGroup)
                else:
                    failed=failed.append(pd.DataFrame({'Host':ind},index=[row.Date+pd.Timedelta('20s')]))
                    failedGroup[ind]=1
              #      print(str(row.Date)+' First Entry ' + ind)
                  #  print(failedGroup)
            elif (ind in failedGroup.index) and (row.Reply==200):
                    failed=failed[failed.Host!=ind]
                    failedGroup=failedGroup[failedGroup.index!=ind]
             #       print(str(row.Date) + ' Success Entry ' + ind)
                
            
        #update files
        
        if row.Date-runtime>=pd.Timedelta('1s'):
            runtime=row.Date
            blocked20=blocked20[blocked20.Time>runtime]
            failed=failed[failed.index>runtime]
            failedGroup=failed['Host'].value_counts()
        #    print(runtime)
        #    print(failedGroup)
         
         
    return f1,blocked20,failed
    





############################################################
#INITIALIZATION

print('Number of arguments:', len(sys.argv), 'arguments.')
print ('Argument List:', str(sys.argv))

if len(sys.argv)==6:
    LOG_FILE=str(sys.argv[1])
    HOSTS_FILE=str(sys.argv[2])
    HOURS_FILE=str(sys.argv[3])
    RESOURCES_FILE=str(sys.argv[4])
    BLOCKED_FILE=str(sys.argv[5])

    print('LOG_FILE=',LOG_FILE)
    print('HOSTS_FILE=',HOSTS_FILE)
    print('HOURS_FILE=',HOURS_FILE)
    print('RESOURCES_FILE=',RESOURCES_FILE)
    print('BLOCKED_FILE=',BLOCKED_FILE)
else:
    LOG_FILE='../log_input/logall.txt' 
    HOSTS_FILE='../log_output/hosts.txt'
    HOURS_FILE='../log_output/hours.txt'
    RESOURCES_FILE='../log_output/resources.txt'
    BLOCKED_FILE='../log_output/blocked.txt'

CHUNKSIZE=100000
t = time.time()
#####Computation Time
tHost=0
tResources=0
tHours=0
tBlocked=0



hosts=pd.DataFrame({'Count':[]}, dtype='int')
Resources=pd.DataFrame({'BandWidth':[]})
Hours=pd.DataFrame()
Blocked=pd.DataFrame()

last60min=pd.DataFrame()

blocked20=pd.DataFrame({'Time':[]})    #the sites that are blocked within the next 20 minutes
failed=pd.DataFrame({'Host':[]})

maxlines=1*CHUNKSIZE  ####TO REMOVE
iline=0       ####TO REMOVE
#try:
for df in pd.read_csv(LOG_FILE,sep=' ', header = None,chunksize=CHUNKSIZE,parse_dates=[3,4],infer_datetime_format=True
                      ,encoding="ISO-8859-1",error_bad_lines=False):
    looptime=time.time()
    t = time.time()
    hosts=countHostsBis(hosts,df)
    tHost=tHost+time.time()-t
    
    t = time.time()
    Resources=BandWidth(Resources,df)
    tResources=tResources+time.time()-t

    t = time.time()
    Hours, last60min=BusiestHoursRolling(Hours,df, last60min)
    tHours=tHours+time.time()-t
    
    t = time.time()
    #Blocked, blocked20, failed=BlockedRequests(Blocked,df,blocked20, failed)
    tBlocked=tBlocked+time.time()-t
    
    iline+=CHUNKSIZE
    print(iline)
    print(time.time() - looptime)
    #if iline >= maxlines:
    #    break
    
print('END READING DATA')
print('Feature 1: computation time ' + str(tHost))
print('Feature 2: computation time ' + str(tResources))
print('Feature 3: computation time ' + str(tHours))
print('Feature 4: computation time ' + str(tBlocked))


 



#######FEATURE 1########
#Sort host Data by 1)active host 2) by lexicographical order
hosts=hosts.to_frame()    #Convert Series to DataFrame for better sorting
hosts.column='Count'
hosts.index.name='idx'
hosts['idx']=hosts.index
hosts=hosts.sort_values(by=['Count','idx'], ascending=[False,True])
hosts=hosts.drop(['idx'], axis=1)
hosts=hosts.astype(int)
hosts[:10].to_csv(HOSTS_FILE,sep=',',header=None)  #Top 10

######FEATURE 2#########
Resources=Resources.to_frame()
Resources.reset_index(inplace=True)
Resources=Resources.sort_values(by=['BandWidth','Domain'], ascending=[False,True])
Resources[:10].Domain.to_csv(RESOURCES_FILE,sep=',',header=None, index=False)  #Top 10


#####FEATURE 3##########

## Doing the last 60 minutes
tmp=df[-1:]
tmp[:][3]='[01/Jul/2100:00:00:41'
Hours, last60min=BusiestHours(Hours,tmp, last60min)

## Building the file
Hours=Hours[:10]
Hours['idx']=Hours.index
Hours=Hours.sort_values(by=[3,'idx'],ascending=[False,True])
Hours.idx=Hours.idx.apply(lambda x: x.strftime("%d/%b/%Y:%X -0400"))
Hours=Hours[['idx',3]]
Hours[3]=Hours[3].astype(int)
Hours.to_csv(HOURS_FILE,sep=',',header=None, index=False)

"""
####FEATURE 4###########
if not Blocked.empty:
    Blocked[6]=Blocked[6].astype(int)
    Blocked[7]=Blocked[7].astype(int)
    Blocked.to_csv(BLOCKED_FILE,sep=' ', header=None, index=False)
else:
    Blocked.to_csv(BLOCKED_FILE,sep=' ', header=None, index=False)
"""
    

