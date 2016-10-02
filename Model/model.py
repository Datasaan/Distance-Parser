# -*- coding: utf-8 -*-
"""
Created on Thu Jun  9 11:36:44 2016

@author: sanjeet
"""
import endtag as ex
import pandas as pd
import boto3 
from cStringIO import StringIO
import numpy as np
s3=boto3.resource('s3')
def read_con(foldername):
    buc=s3.Bucket('hotel.nikki.ai')
    l=[]
    for item in buc.objects.filter(Prefix=foldername):
        if '.conll' in item.key:        
            l.append(item.key)
    return {'list':l,'bucket':buc}
def get_df(key,bucket='hotel.nikki.ai'):
    resp = s3.Object(bucket_name=bucket,key=key).get()
    ob=StringIO(resp['Body'].read())
    a=pd.read_table(ob,header=None)
    return a.drop(a.columns[[2,5,8,9]],axis=1)
def loop_con(ewno,dwno,temp):
    e=temp[temp['wno']==ewno]
    d=temp[temp['wno']==dwno]
    eo=temp[temp['wno']==ewno]
    do=temp[temp['wno']==dwno]
    while(eo['con'].iloc[0]!=0):
        eo=temp[temp['wno']==eo['con'].iloc[0]]
        e=pd.concat([e,eo])
    while(do['con'].iloc[0]!=0):
        do=temp[temp['wno']==do['con'].iloc[0]]
        d=pd.concat([d,do])
    d=d.iloc[::-1]
    df=pd.concat([e,d])
    torm=df['wno'].value_counts()[(df['wno'].value_counts()==2)].index
    cond=~(df['wno'].isin(torm))
    cond.iloc[0]=True
    cond.iloc[-1]=True
    df=df[cond]
    feature=pd.Series()
    feature['sent']=temp['nword'].str.cat(sep=' ')
    feature['entity']=df['word'].iloc[0]
    feature['distance']=df['word'].iloc[-1]
    feature['word_dist']=df['wno'].iloc[0]-df['wno'].iloc[-1]
    feature['wno_order']=df['wno'].values
    feature['npos_order']=df['npos'].values
    feature['type_order']=df['type'].values
    feature['con_order']=df['con'].values
    feature['pos_order']=df['pos'].values
    return feature
def relfilt(temp):
    if sum(pd.Series(temp['tag'].unique()).isin([1,0]))!=2:
        temp=pd.DataFrame()
    return temp
def run_file(key,bucket='hotel.nikki.ai'):
        a=get_df(key,bucket)
        i=a[a.iloc[:,0]==1].index.values
        i=np.append(i,len(a))
        dfm=pd.DataFrame()
        for j in range(0,len(i)-1):
            temp=pd.DataFrame()
            temp=a[i[j]:i[j+1]]
            temp.columns=['wno','nword','npos','type','con','pos']
            temp=ex.compound_words(temp)
            temp=relfilt(temp)
            fet=pd.DataFrame()
            if not temp.empty:
                entity=temp[temp['tag']==1]['wno']
                distance=temp[temp['tag']==0]['wno']
                endi=pd.DataFrame(pd.tools.util.cartesian_product([entity,distance])).T
                fet=endi.apply((lambda x: loop_con(x.iloc[0],x.iloc[1],temp)),axis=1)
                dfm=pd.concat([dfm,fet])
        return dfm

l=read_con('IHL18F')['list']
dfm=pd.DataFrame()
dataset1=pd.DataFrame()
bucket='hotel.nikki.ai'
for u in range(0,len(l)):
    print(u)
    key=l[u]
    fr=run_file(key,bucket)
    fr['id']=key
    dataset1=pd.concat([dataset1,fr])