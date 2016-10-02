# -*- coding: utf-8 -*-
"""
Created on Thu May 26 11:03:32 2016

@author: dharmendra
"""
import re
import random
import string
#Method to split string into sentences
caps = "([A-Z])"
Num= "([0-9])"
prefixes = "(Mr|St|Mrs|Ms|Dr|etc|Inc|Pvt|Ltd|Jr|Sr|Co)[.]"
websites = "[.](com|net|org|io|gov)"
def split_into_sentences(text):
    ln=len(text)
    if(text[ln-1]!='.'):
        text=text+'.'
    text = " " + text + "  "
    text = text.replace("\n"," ")
    text=text.replace("\\n","")
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
    text = text.replace(".","<stop>")
    text = text.replace("?","<stop>")
    text = text.replace("!","<stop>")
    text = text.replace("<prd>",".")
    sentences = text.split("<stop>")
    sentences = sentences[:-1]
    sentences = [s+'.' for s in sentences]
    return sentences
#method to generate random id
def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))
#method to return lst of ids and corresponding list of sentences
def sentences(str1):
#str1='Sequence objects may be compared to other objects with the same sequence type. The comparison uses lexicographical ordering: first the first two items are compared, and if they differ this determines the outcome of the comparison; if they are equal, the next two items are compared, and so on, until either sequence is exhausted. If two items to be compared are themselves sequences of the same type, the lexicographical comparison is carried out recursively. If all items of two sequences compare equal, the sequences are considered equal. If one sequence is an initial sub-sequence of the other, the shorter sequence is the smaller (lesser) one. Lexicographical ordering for strings uses the Unicode codepoint number to order individual characters.'
    str1=re.sub('<[^>/]*>','',str1)
    str1=re.sub('<[^>]*>','.',str1) 
    str1=str1.split('id:')
    ids=[]
    desc=[]
    results =[]
    for i  in range (len(str1)):
        if(len(str1[i])!=1):
            st=str1[i].split('\n')
            result={}
            if(len(st)>=2):
                result['id']=st[0].strip()
                result['sentences']=split_into_sentences(st[1].strip())
            elif(len(st)==1):
                result['id']=id_generator()
                result['sentences']=split_into_sentences(st[0].strip())
            results.append(result)
    return(results)
#print(id_generator())
if __name__=='__main__' :
    fp=open('Hoteldesc.txt','r')
    str1=fp.read() 
    print(sentences(str1))



      
        