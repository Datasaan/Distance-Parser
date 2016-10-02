# Organising the Hotel description data in a data frame 
# -*- coding: utf-8 -*-
"""
Created on Sat May 14 18:54:58 2016

@author: Sanjeet 

"""

import pandas as pd 
import numpy as np 
import re
import StringIO

def read_stringio(siof,idnew):        
    a=siof.read().split("\n")    
    a=filter(lambda x: len(x)>0,a)
    a=pd.Series(a)
    cond=sum(a.apply(lambda x:bool(re.search('[iI][dD]:',x))))
    b=pd.Series()
    if cond:
        b=a.loc[4*(np.unique(np.arange(len(a))/4))]
        b.index=b.index/4
        b=b.str.replace("Hotel id:","")
        b=b.str.replace("\r","")
        b=b.str.replace(" ","")
        c=a[4*(np.unique(np.arange(len(a))/4))+1]
        c.index=c.index/4
    else:
        b=pd.Series([idnew])
        c=a    
    c=c.str.replace("Description:","<b>Description</b>")
    c=c.str.split("<b>")
    g=c.apply(lambda x: pd.Series(x))
    if len(g)>1:
        g.drop(g.columns[[0]], axis=1, inplace=True)
    g=g.apply((lambda x: x.str.split("</b>")),axis=0)
    main=pd.DataFrame()
    for i in range(0,(len(g))) :
        t=g.iloc[i,:].apply(lambda x: pd.Series(x))
        k=pd.Series([b[i]]*9)
        t=pd.concat([k,t],axis=1)
        main=pd.concat([main,t])
    if main.shape[1]==2:
        main.columns=['id','tag_data']
        main.iloc[:,1]=main.iloc[:,1].str.strip()
        o1=main.iloc[:,1].apply(lambda x: bool(re.search('\b?\(?[kK][mM]([sS])?\)?\b?| \b?\(?[mM][sS]?\)?\b?| [Cc]lose(st)? |[mM](inute)(s)?| [nN]ear(est)? | [dD]istance | [oO]pposite | [aA]way |\b?\(?[mM]in(s)?\)?\b? |\b?\(?[hH]our(s)?\)?\b?',str(x))))
    if main.shape[1]==3:
        main.columns=['id','tag','tag_data']    
        main.iloc[:,1]=main.iloc[:,1].str.strip().str.lower().str.replace(":","")
        main.iloc[:,2]=main.iloc[:,2].str.strip()
        p=main.iloc[:,1].str.len()    
        p=p.apply(lambda x : float(x)>30)
        main.iloc[:,2][p]=main[p]['tag']
        main.iloc[:,1][p]=pd.Series(['description2']*len(main.iloc[:,1][p]))
        main.iloc[:,1]=main.iloc[:,1].str.strip().str.lower().str.replace(":","")
        main.iloc[:,2]=main.iloc[:,2].str.strip()
        #main.iloc[:,1].value_counts()
        o1=main.iloc[:,2].apply(lambda x: bool(re.search('\b?\(?[kK][mM]([sS])?\)?\b?| \b?\(?[mM][sS]?\)?\b?| [Cc]lose(st)? |[mM](inute)(s)?| [nN]ear(est)? | [dD]istance | [oO]pposite | [aA]way |\b?\(?[mM]in(s)?\)?\b? |\b?\(?[hH]our(s)?\)?\b?',str(x))))
    main=pd.concat([main,o1],axis=1)
    if main.shape[1]==4:
        main.columns=['id','tag','tag_data','rel1']
    if main.shape[1]==3:   
        main.columns=['id','tag_data','rel1']
    #print(main)
    #main.to_csv('hotels3.csv')
    # Status:: Need to work on cases where tags are absent
    # Status:: Need to remove NaN values
    #-----------------------------------------------------------------------------------------------
    #-------------------------------------------------------------------------------------------------------------
    return main
    
def split_into_sentences(text):
    caps = "([A-Z])"
    Num= "([0-9])"
    prefixes = "(Mr|St|Mrs|Ms|Dr|etc|Inc|Pvt|Ltd|Jr|Sr|Co)[.]"
    websites = "[.](com|net|org|io|gov)"
    text = " " + text + "  "
    text = text.replace("\n"," ")
    text = re.sub(prefixes,"\\1<prd>",text)
    text = re.sub(websites,"<prd>\\1",text)
    text= re.sub(Num+"[.]","\\1<prd>",text)
    text = re.sub("\s" + caps + "[.] "," \\1<prd> ",text)
    text = re.sub(caps + "[.]" + caps + "[.]" + caps + "[.]","\\1<prd>\\2<prd>\\3<prd>",text)
    text = re.sub(caps + "[.]" + caps + "[.]","\\1<prd>\\2<prd>",text)
    if "i.e" in text: text=text.replace("i.e.","i<prd>e<prd>")
    if "e.g" in text: text=text.replace("e.g.","e<prd>g<prd>")
    if "”" in text: text = text.replace(".”","”.")
    if "\"" in text: text = text.replace(".\"","\".")
    if "!" in text: text = text.replace("!\"","\"!")
    if "?" in text: text = text.replace("?\"","\"?")
    text=text.replace(",",",")
    text = text.replace(".",".<stop>")
    text = text.replace("?","<stop>")
    text = text.replace("!","<stop>")
    text = text.replace("<prd>",".")
    if "<stop>" not in text:
        text+="<stop>"
    sentences = text.split("<stop>")
    sentences = sentences[:-1]
    sentences = [s.strip() for s in sentences]
    return sentences
#--------------------------------------------------------------------------------
#--------------------------------------------------------------------------------



def sentence_spt(str1): 
    pt=split_into_sentences(str(str1))
    return pd.Series(pt)


def sentxt(id,main):
    a=main['tag_data'][(main['id']==id) & (main['rel1'])]
    f=pd.Series()  
    for i in a:
        b=sentence_spt(i)
        f=pd.concat([f,b])
    print f
    pat='\b?\(?[kK][mM]([sS])?\)?\b?| \b?\(?[mM][sS]?\)?\b?| [Cc]lose(st)? |[mM](inute)(s)?| [nN]ear(est)? | [dD]istance | [oO]pposite | [aA]way |\b?\(?[mM]in(s)?\)?\b? |\b?\(?[hH]our(s)?\)?\b?'
    cond=f.apply(lambda x: bool(re.search(pat,str(x))))
    if not f[cond].empty:
        f[cond].to_csv(id+'.txt',index=None,sep='"',header=None)
    return
 
def read_tag(siof,idnew=' '):
    main=read_stringio(siof,idnew)
    pd.Series(main['id'].unique()).apply(lambda x:sentxt(x,main))
    return
    
if __name__ == "__main__":

    siof=StringIO.StringIO('Hotelgeghfbjafiaifsa0fufhijf km ihfiachp.Hello Km min AM here.\n Hi i am here min')    
    read_tag(siof,'faf')
    