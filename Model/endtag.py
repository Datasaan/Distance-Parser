# -*- coding: utf-8 -*-
"""
Created on Fri May 27 17:19:22 2016

@author: dharmendra
"""
import pandas as pd
import csv
import re
class Stack:
    def __init__(self):
        self.stack=[]
        self.hasnnp=False
    def push(self,word):
        self.stack.append(word)
    def pop(self):
        st=''
        while(len(self.stack)!=0):
            st+=self.stack.pop(0)+' '
        self.hasnnp=False
        return(st)
    def clear(self):
        del self.stack[:]
        self.hasnnp=False
    def setNNP(self):
        self.hasnnp=True
    def hasNNP(self):
        return(self.hasnnp)
    def hasWord(self):
        if(len(self.stack)>=1):
            return(True)
        return(False)
def compound_words(temp):
    lst=pd.DataFrame.copy(temp)
    lst['word']=''
    lst['tag']=''
    poslist=['NNP','NNPS','NFP','NP','NPS']
    NNList=['NN','NNS']
    dis=['Km','km','m','M','minutes','minute','hours','Hours','mins','Mins','Minute','Minutes','kms',
         'ms','Kms','Ms']
    prep=['of','&','-']
    dis2=['close','Close','closer','nearer','closest','Closest','Near','near','Nearest','nearest','opposite','Opposite']
    stack=Stack()
    cnnlist=['airport','metro','station','hospital']
    dis1=['CD','DT']
    i=0
    while(i<len(lst)):
        lst.iloc[i,-2]=lst.iloc[i,1]
        if(lst.iat[i,1] in dis2 or bool(re.match('[\d/.]+(\ )?(-)?(km|kms|Km|Kms|M|Ms|m|ms|Mins|mins|Min|min|Minute|Minute|minutes|minute|Hours|Hour|hours|hour)',str(lst.iloc[i,1])))):
          lst.iat[i,-2]=lst.iloc[i,1]
          lst.iat[i,-1]=0  
        elif((i+1)<len(lst) and lst.iloc[i,3] in dis1 and lst.iloc[i+1,1] in dis):
            lst.iat[i+1,-2]=lst.iloc[i,1]+' '+lst.iloc[i+1,1]
            lst.iat[i+1,-1]=0
            i=i+1
        if(lst.iloc[i,1]in cnnlist):
            lst.iloc[i,3]='NNP'
        if(lst.iloc[i,3] in NNList or lst.iloc[i,1] in prep):
           stack.push(lst.iloc[i,1])
			
        elif(lst.iloc[i,3] in poslist):
            stack.push(lst.iloc[i,1])
            stack.setNNP()
        else:
            if(stack.hasNNP()):
                lst.iat[i-1,-2]=stack.pop()
                lst.iat[i-1,3]='NNP'
                lst.iat[i-1,-1]=1
            else:
                stack.clear()
        i+=1
    if(stack.hasNNP()):
        lst.iat[-1,-2]=stack.pop()
        lst.iat[-1,3]='NNP'
        lst.iat[-1,-1]=1
    else: 
        stack.clear()
    return(lst)

