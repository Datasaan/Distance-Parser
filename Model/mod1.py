# -*- coding: utf-8 -*-
"""
Created on Fri Jun 10 13:26:20 2016

@author: sanjeet
"""
import pandas as pd 
import sklearn as sk
from sklearn.ensemble import RandomForestClassifier as rf
import sklearn.cross_validation as cv
import sklearn.feature_extraction as fe
#d1=pd.read_csv('dharm.csv',header=0)
#d2=pd.read_csv('sanjeet.csv',header=0)
#d3=pd.read_csv('vrushab.csv',header=0)
#d=pd.concat([d1,d2,d3])
#dataset1['id']=dataset1['id'].str.replace('IHL18F/','').str.replace('.conll','').str.strip()
#data=dataset1[dataset1['id'].isin(d.id.unique())]
#data['target']=np.zeros(len(data))+4
#data['entity']=data['entity'].str.strip()
#d['Places']=d['Places'].str.strip()
#d['distance']=d['distance'].str.replace('K','k').str.strip()
#cond=data['entity'].isin(d['Places'])
#def tagdata(x):
#    reld=d[d['id']==x['id']]
#    reld=reld[reld['Places']==x['entity']]
#    if x['distance'] in reld['distance'].values:
#        x['target']=1
#    else:
#        x['target']=0
#    return x
#tdata=data.apply(tagdata,axis=1)
#tdata['type_order']=tdata['type_order'].apply(str)
#tdata['pos_order']=tdata['pos_order'].apply(str)
tdata=pd.read_excel('firstdf.xlsx')
tdata=tdata.iloc[:,0:11]
def cleaner(tdata):
    sigpos=(tdata['pos_order'].value_counts()>20).index
    sigtype=(tdata['type_order'].value_counts()>20).index
    tdata=tdata[tdata['pos_order'].isin(sigpos)]
    tdata=tdata[tdata['type_order'].isin(sigtype)]
    return tdata
def str2num(text):
   import hashlib
   return int(hashlib.md5(text).hexdigest()[:8], 16)
def inputer(tdata):
    m=pd.DataFrame()
    m['pos_order']=pd.Categorical(tdata['pos_order'])
    m['pos_order'].cat.categories=pd.Series(m['pos_order'].cat.categories).apply(str2num)
    m['type_order']=pd.Categorical(tdata['type_order'])
    m['type_order'].cat.categories=pd.Series(m['type_order'].cat.categories).apply(str2num)
    m['word_dist']=pd.Categorical(tdata['word_dist'])
    #m['abs_word_dist']=pd.Categorical(abs(tdata['word_dist']))
    #m['sign_word_dist']=list(tdata['word_dist']/abs(tdata['word_dist']))
    return m
def outputer(tdata):
    y=pd.DataFrame()
    y['target']=pd.Categorical(tdata['target'])
    return y
x_t,x_te,y_t,y_te=cv.train_test_split(inputer(cleaner(tdata)),outputer(cleaner(tdata)),test_size=0.4)
r=rf()
r.fit(x_t,y_t)
print r.score(x_te,y_te)
tdata['target_1']=r.predict(inputer(tdata))
#Notes
# RF p,t,wd ---91
# RF p,t,awd,swd,----91
#Rf p,awd,swd------