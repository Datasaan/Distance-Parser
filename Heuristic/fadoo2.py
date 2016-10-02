# -*- coding: utf-8 -*-

"""
Created on Fri May 27 00:00:39 2016

@author: sanjeet
"""

import ext as ex
import pandas as pd
import boto3 
from cStringIO import StringIO
import re 
import urllib
import numpy as np
s3=boto3.resource('s3')

def read_con(foldername):
    
    buc=s3.Bucket('hotel.nikki.ai')
    l=[]
    for item in buc.objects.filter(Prefix=foldername):
        if '.conll' in item.key:        
            l.append(item.key)
    return {'list':l,'bucket':buc}
def findsub(t,temp):
    df=t.apply(lambda x: insidesub(x,temp),axis=1)
    nnp=temp[temp['pos'].isin(['nsubj','nsubjpass'])]
    #nnp=temp[temp['type'].isin(['NNP','NNS'])]
    t['subject']=df_searcher(df,'wno',nnp,'con','word')
    t['subjecttype']=df_searcher(df,'wno',nnp,'con','type')
    t['msubject']=df_searcher(df,'con',nnp,'con','word')
    t['msubjecttype']=df_searcher(df,'con',nnp,'con','type')
    return t
def insidesub(d,temp):    
    prev=d 
    while(1):
        e=temp[temp['wno']==d['con']]
        if(len(e)>0):
            d=e.iloc[0]
            prev=d
            if(d['npos']=='ROOT'):
                break
            elif(d['npos']=='VERB'):
                break
        else:
            break
    return prev 
def count_test(temp):
        pat='OYO'
        numcounte=0
        entcounte=0
        l=[]
        z1=temp[temp['pos']=='num']
        
        for k in range(0,len(temp)):
            if(len(temp.iloc[k,-1]))>=1:
                if(pat not in temp.iloc[k,-1]):
                    entcounte+=1
                    l.append(temp.iloc[k,-1])
                #  print temp.iloc[k,-1]
                
        for ind in z1.index:
            if (temp.loc[ind+1]['pos']=='appos')|(temp.loc[ind+1]['pos']=='pobj')|(temp.loc[ind+1]['pos']=='npadvmod')|(temp.loc[ind+1]['pos']=='conj'):
                        numcounte+=1
        return [numcounte,l]
def df_searcher(df1,col1,df2,col2,col):
    t=(df1[col1].apply(lambda x: 'NaN' if x=='NaN' else 'NaN' if df2[df2[col2]==x][col].empty else df2[df2[col2]==x][col].iloc[0])).values 
    return t
def get_df(key,bucket='hotel.nikki.ai'):
    resp = s3.Object(bucket_name=bucket,key=key).get()
    ob=StringIO(resp['Body'].read())
    a=pd.read_table(ob,header=None)
    return a.drop(a.columns[[2,5,8,9]],axis=1)
def prep_con(temp):
    t=pd.DataFrame.copy(temp[temp['pos'].isin(['prep','advmod'])])
    p=temp[temp['pos'].isin(['pobj','pcomp'])]
    nnp=pd.DataFrame.copy(temp[temp['pos'].isin(['num','det'])])
    nonp=pd.DataFrame.copy(temp[temp['pos'].isin(['npadvmod','ccomp','num','amod'])])
    t['sub']=df_searcher(t,'con',temp,'wno','word')
    #    t['prev']=(temp[temp['wno'].isin(t['wno']-1)]['word']).values
    #  t['prevwno']=(temp[temp['wno'].isin(t['wno']-1)]['wno']).values
    #    t['prevmod']=df_searcher(t,'prevwno',nnp,'con','word')
    t['subwno']=df_searcher(t,'con',temp,'wno','wno')
    t['subcon']=df_searcher(t,'con',temp,'wno','con')
    t['prevsub']=df_searcher(t,'subwno',nonp,'con','word')
    t['prevsubwno']=df_searcher(t,'subwno',nonp,'con','wno')
    t['subprep']=df_searcher(t,'subwno',t,'con','word')
    t['subprepwno']=df_searcher(t,'subwno',t,'con','wno')
    t['subprepobj']=df_searcher(t,'subprepwno',p,'con','word')
    t['subprepobjwno']=df_searcher(t,'subprepwno',p,'con','wno')
    t['subprepobjmod']=df_searcher(t,'subprepobjwno',nnp,'con','word')
    t['prevsubnum']=df_searcher(t,'prevsubwno',nnp,'con','word')
    t['subnpos']=df_searcher(t,'con',temp,'wno','npos')
    t['subtype']=df_searcher(t,'con',temp,'wno','type')
    t['submod']=df_searcher(t,'subwno',nnp,'con','word')
    t['submodtype']=df_searcher(t,'subwno',nnp,'con','type')
    t['npobj']=df_searcher(t,'wno',nonp,'con','word')
    t['npobjwno']=df_searcher(t,'wno',nonp,'con','wno')
    t['npobjmod']=df_searcher(t,'npobjwno',nnp,'con','word')
    t['obj']=df_searcher(t,'wno',p,'con','word')
    t['objwno']=df_searcher(t,'wno',p,'con','wno')
    t['objcon']=df_searcher(t,'wno',p,'con','con')
    t['objpos']=df_searcher(t,'wno',p,'con','pos')
    t['objtype']=df_searcher(t,'wno',p,'con','type')
    t['objmod']=df_searcher(t,'objwno',nnp,'con','word')
    t['objmodtype']=df_searcher(t,'objwno',nnp,'con','type')
    return t
def relfilt(temp):
    pat=r'\b[0-9]*-?[Mm]\b|\b[0-9]*-?[Kk][mM]\b|\b[Cc]lose(st)?\b|\b[0-9]*-?[Mm]in(ute)?(s)?\b|\b[Nn]ear(est)?\b|\b[Oo]pposite\b|\b[Aa]way\b|\b[Mm]ins?\b|\b[Hh]ours?\b'
    temp['word']=temp['word'].str.strip()
    cond=sum(temp['word'].apply(lambda x: bool(re.match(pat,x))))
    if cond==0:
        temp=pd.DataFrame()
    return temp
def distancerec(a):
    pat=r'\b[0-9]*[Kk][Mm]s?\b|\b[0-9]*ms?\b|\b[0-9]*[Mm]in(ute)?s?\b|\b[Cc]loser?(st)?\b|\bnear(er)?(est)?\b|\bnext\b|\bop+osite\b|\badjacent\b|\b([wW]ell)?-?[Cc]onnected\b'
    cond=a.str.contains(pat)
    return [a[cond].values]
    
def adddist(df):
    df['submod']=df['submod'].apply(lambda x: str(x).replace('nan','').replace('NaN',''))
    df['sub']=df['sub'].apply(lambda x: str(x).replace('nan','').replace('NaN',''))
    df['obj']=df['obj'].apply(lambda x: str(x).replace('nan','').replace('NaN',''))
    df['objmod']=df['objmod'].apply(lambda x: str(x).replace('nan','').replace('NaN',''))
    df['prevsub']=df['prevsub'].apply(lambda x: str(x).replace('nan','').replace('NaN',''))
    df['prevsubnum']=df['prevsubnum'].apply(lambda x: str(x).replace('nan','').replace('NaN',''))
    df['dist1']=df['objmod']+' '+df['obj']
    df['dist2']=df['submod']+' '+df['sub']
    df['dist3']=df['prevsubnum']+' '+df['prevsub']
    df['dist4']=df['subprepobjmod']+' '+df['subprepobj']
    df['dist5']=df['npobjmod']+' '+df['npobj']
    df['distance']=df[['dist1','dist2','dist3','dist4','dist5']].apply((lambda x:distancerec(x)),axis=1)
    return df
def addplace(x):
    x['place']=''
    pat=r'Km|[oO][Yy][Oo].*'
    for i in range(0,len(x)):
        l=[]
        if(x['subtype'].iloc[i]=='NNP'):
            if not (bool(re.match(pat,x['sub'].iloc[i]))):
              l.append(x['sub'].iloc[i])
        if(x['objtype'].iloc[i]=='NNP'):
            if not (bool(re.match(pat,x['obj'].iloc[i]))):
              l.append(x['obj'].iloc[i])
        if(x['subjecttype'].iloc[i]=='NNP'):
            if not (bool(re.match(pat,x['subject'].iloc[i]))):
              l.append(x['subject'].iloc[i])
        x['place'].iat[i]=l
    return x
def findaway(temp):
    nonp=pd.DataFrame.copy(temp[temp['pos'].isin(['num','det'])])   
    pfw=pd.DataFrame.copy(temp[temp['word']=='away'])
    pfwf=pd.DataFrame.copy(temp[temp['wno'].isin(pfw['wno']+1)])
    pfwf=pd.DataFrame.copy(pfwf[pfwf['word']!='from'])
    pfw=pd.DataFrame.copy(temp[temp['wno'].isin(pfwf['wno']-1)])
    paway=pd.DataFrame()
    if pfw.empty==False:
        nnp=pd.DataFrame.copy(temp[temp['npos']=='NOUN'][temp['pos'].isin(['nsubj','conj'])])
        pfw['sub']=df_searcher(pfw,'con',nnp,'con','word')   
        pfw['submod']=(temp[temp['wno'].isin(pfw['wno']-1)]['word']).values
        pfw['submodwno']=(temp[temp['wno'].isin(pfw['wno']-1)]['wno']).values
        pfw['submodnum']=df_searcher(pfw,'submodwno',nonp,'con','word')
        paway['place']=pfw['sub']
        paway['distance']=pfw['submodnum']+' '+pfw['submod']
    return paway
def filtdp(dfm):
    for i in range(0,len(dfm)):
        a=pd.Series(dfm['place'].iloc[i])
        if not len(dfm['distance'].iloc[i][0])==0:
            dfm['distance'].iloc[i]=dfm['distance'].iloc[i][0][0]
        if not a.empty:
            dfm['place'].iloc[i]=a[a.str.len()==max(a.str.len())].iloc[0]
    return dfm
def test():
    l=read_con('IHL18F')['list']
    dfm=pd.DataFrame()
    place_list=pd.DataFrame()
    for u in range(0,len(l)):
        print(u)
        a=get_df(l[u])
        key=l[u]
        i=a[a.iloc[:,0]==1].index.values
        i=np.append(i,len(a))
        #a=pd.read_table('',header=None)
        #a=a.drop(a.columns[[2,5,8,9]],axis=1)
        #idl=listid.iloc[:,1]
        for j in range(0,len(i)-1):
            temp=pd.DataFrame()
            df=pd.DataFrame()
            temp=a[i[j]:i[j+1]]
            temp.columns=['wno','nword','npos','type','con','pos']
            temp=ex.compound_words(temp)
            gp=pd.DataFrame()
            paway=pd.DataFrame()
            gp[['place','distance']]=temp[temp['BracketDistance'].str.len()>0][['word','BracketDistance']]
            temp=relfilt(temp)
            sd=pd.DataFrame()
            if temp.empty==False:
                    t=prep_con(temp)
                    ec=count_test(temp)
                    paway=findaway(temp)
                    df=findsub(t,temp)
                    if not df.empty:
                        df=adddist(df)
                        df=addplace(df)
                        df=filtdp(df)
                        df=df[(df['distance'].apply(len)!=1)&(df['place'].apply(len)!=0)]
                        sd=df[['place','distance']]
                        dfm=pd.concat([dfm,df])
            m=pd.concat([gp,sd,paway])
            fg=pd.DataFrame()
            #            if len(m)==1:
            #                fg['place']=pd.Series(ec[1])
            #                fg['distance']=m['distance'].iloc[0]
            #                m=fg
            #print(j)
            #idstr=idl.iloc[j]
            idstr=key.replace('IHL18F','').replace('.conll','')
            m['id']=idstr
            place_list=pd.concat([place_list,m])
    return
def run_file(key,bucket='hotel.nikki.ai'):
        a=get_df(key,bucket)
        i=a[a.iloc[:,0]==1].index.values
        i=np.append(i,len(a))
        place_list=pd.DataFrame()
        dfm=pd.DataFrame()
        for j in range(0,len(i)-1):
            temp=pd.DataFrame()
            df=pd.DataFrame()
            temp=a[i[j]:i[j+1]]
            temp.columns=['wno','nword','npos','type','con','pos']
            temp=ex.compound_words(temp)
            gp=pd.DataFrame()
            paway=pd.DataFrame()
            gp[['place','distance']]=temp[temp['BracketDistance'].str.len()>0][['word','BracketDistance']]
            temp=relfilt(temp)
            sd=pd.DataFrame()
            if temp.empty==False:
                    t=prep_con(temp)
                    ec=count_test(temp)
                    paway=findaway(temp)
                    df=findsub(t,temp)
                    if not df.empty:
                        df=adddist(df)
                        df=addplace(df)
                        df=filtdp(df)
                        df=df[(df['distance'].apply(len)!=1)&(df['place'].apply(len)!=0)]
                        sd=df[['place','distance']]
                        dfm=pd.concat([dfm,df])
            m=pd.concat([gp,sd,paway])
            fg=pd.DataFrame()
#            if len(m)==1 and ec[0]==1:
#                fg['place']=pd.Series(ec[1])
#                fg['distance']=m['distance'].iloc[0]
#                m=fg
            idstr=str(key.split('/')[1].replace('.conll',''))
            keyid=str(key.split('/')[0].replace('.conll',''))
            m['id']=idstr
            place_list=pd.concat([place_list,m])
        di=place_list[['place','distance']].to_dict(orient='records')
        did = {}
        did['results']=di
        did['id']=idstr
        UpdateResultsDynamoDB(keyid,did['id'],'results',did['results'])
        return did

def lambda_handler(event, context):
   bucket = event['Records'][0]['s3']['bucket']['name']
   key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key']).decode('utf8')
   obj = run_file(key,bucket)
   return obj

def UpdateResultsDynamoDB(key,id,attribute,value):
   table = boto3.resource('dynamodb').Table('niki')
   result = table.update_item(
       Key={
           'key':key,
           'id':id
       },
    UpdateExpression= 'SET #attribute = list_append(if_not_exists(#attribute,:d),:i)',
       ExpressionAttributeNames={
          '#attribute':attribute,
       },
    ExpressionAttributeValues={
           ':d': [ ],
           ':i': value
       }
   )
   
