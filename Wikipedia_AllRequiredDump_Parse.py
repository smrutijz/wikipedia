#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import codecs
import csv
import gzip
import bz2
import xml.etree.ElementTree as etree
import time


# In[2]:


dump_path = r"C:\Users\S5SBS7\Desktop\work\wikipedia"
save_path = r"C:\Users\S5SBS7\Desktop\work\wikipedia\csvs"


# ### 1. Wikipedia_Page_ID to WikiData_Item
# Here we will read mysql file using python
# ##### Link
# https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-page_props.sql.gz
# ##### Requirement
# enwiki-latest-page_props.sql.gz

# In[ ]:


enwiki_latest_page_props_sql = "enwiki-latest-page_props.sql.gz"
filename = os.path.join(dump_path,enwiki_latest_page_props_sql)

WP_PageID_2_WP_WikiData_Item = "WP_PageID_2_WP_WikiData_Item.csv"
opfilename = os.path.join(save_path,WP_PageID_2_WP_WikiData_Item)



with codecs.open(opfilename, "w", "utf-8") as op:
    opwriter = csv.writer(op, quoting=csv.QUOTE_MINIMAL)
    opwriter.writerow(["WP_Page_ID", "WP_WikiData_Item"])

    with gzip.open(filename, "rb") as f:
        for line in f:
            line = line.decode('utf_8',errors="ignore")
            
            if line.startswith("INSERT INTO `page_props` VALUES"):
                entries = line[32:].split("),(")
                
                for entry in entries:
                    fields = entry.strip("(").strip(")").replace(");","").split(",")
                    if fields[1] == "'wikibase_item'":
                        opwriter.writerow([fields[0], fields[2].strip("'")])
                        


# ### 2. Wikipedia_Title to Wikipedia_URL
# Here we will read xml file using python. This file is originally for getting the abstract. However, we have noticed in many cases the abstract text are missing. Hence, we will use this to data dump to extract Wikipedia Title and URL/Link.
# ##### Link
# https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-abstract.xml.gz
# ##### Requirement
# enwiki-latest-abstract.xml.gz

# In[ ]:


enwiki_latest_abstract_xml = "enwiki-latest-abstract.xml.gz"
filename = os.path.join(dump_path,enwiki_latest_abstract_xml)

WP_Title_2_WP_URL = "WP_Title_2_WP_URL.csv"
opfilename = os.path.join(save_path,WP_Title_2_WP_URL)


i=0
article_count = 0

with codecs.open(opfilename, "w", "utf-8") as op:
    opwriter = csv.writer(op, quoting=csv.QUOTE_MINIMAL)
    opwriter.writerow(["WP_Title", "WP_URL"])
    
    for event, elem in etree.iterparse(gzip.open(filename,"r"), events=("start", "end")):       
        elem_tag = elem.tag
        elem_text = elem.text
        
        if event == "end":
            if elem_tag== "title":
                title = elem.text.replace("Wikipedia: ","")
            elif elem_tag == "url":
                url = elem.text
            elif elem_tag == "doc":
                article_count += 1
                opwriter.writerow([title, url])   
    elem.clear()


# ### 3. Wikipedia_Page_ID to Wikipedia_Title
# ### 4. Wikipedia_Redirect_Title to Wikipedia_Title
# ### 5. Wikipedia_Title to Wikipedia_Text
# Here we will read xml file using python.
# ##### Link
# https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2
# ##### Requirement
# enwiki-latest-pages-articles.xml.bz2

# In[3]:


# Nicely formatted time string
def hms_string(sec_elapsed):
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60
    return "{}:{:>02}:{:>05.2f}".format(h, m, s)

def strip_tag_name(t):
    idx = t.rfind("}")
    if idx != -1:
        t = t[idx + 1:]
    return t


# In[4]:


enwiki_latest_pages_articles_xml = "enwiki-latest-pages-articles.xml.bz2"
filename = os.path.join(dump_path, enwiki_latest_pages_articles_xml)

Articles = "WP_Page_ID_2_WP_Title.csv"
pathArticles = os.path.join(save_path, Articles)

ArticlesRedirect = "WP_Redirect_Title_2_WP_Title.csv"
pathArticlesRedirect = os.path.join(save_path, ArticlesRedirect)

ArticlesText = "WP_Page_ID_2_WP_Text.csv"
pathText = os.path.join(save_path, ArticlesText)




totalCount = 0
articleCount = 0
redirectCount = 0
wikiCount = 0
title = None
list_of_Namespace = list()




start_time = time.time()

with codecs.open(pathArticles, "w", "utf-8") as articlesFH, codecs.open(pathArticlesRedirect, "w", "utf-8") as redirectFH, codecs.open(pathText, "w", "utf-8") as textFH:
    
    articlesWriter = csv.writer(articlesFH, quoting=csv.QUOTE_MINIMAL)
    redirectWriter = csv.writer(redirectFH, quoting=csv.QUOTE_MINIMAL)
    textWriter = csv.writer(textFH, quoting=csv.QUOTE_MINIMAL)

    articlesWriter.writerow(["WP_Page_ID", "WP_Title", "WP_Namespace"])
    redirectWriter.writerow(["WP_Redirect_Page_ID", "WP_Redirect_Title", "WP_Redirect_Namespace", "WP_Title"])
    textWriter.writerow(["WP_Page_ID", "WP_Text"])

    for event, elem in etree.iterparse(bz2.BZ2File(filename, "r"), events=("start", "end")):
        tname = strip_tag_name(elem.tag)

        if event == "start":
            if tname == "page":
                title = ""
                id = -1
                redirect = ""
                inrevision = False
                ns = 0
                wiki_text = ""
            elif tname == "revision":
                # Do not pick up on revision id's
                inrevision = True
        else:
            if tname == "title":
                title = elem.text
            elif tname == "id" and not inrevision:
                id = int(elem.text)
            elif tname == "redirect":
                redirect = elem.attrib['title']
            elif tname == "ns":
                ns = int(elem.text)
                list_of_Namespace.append(ns)
            elif tname == "text":
                wiki_text = elem.text
                if wiki_text is not None:
                    wiki_text = wiki_text
                    
            elif tname == "page":
                totalCount += 1

                if len(redirect) > 0:
                    redirectCount += 1
                    redirectWriter.writerow([id, title, ns, redirect])
                else:
                    articleCount += 1
                    articlesWriter.writerow([id, title, ns])
                    if ns==0:
                        wikiCount += 1
                        textWriter.writerow([id, wiki_text])

                ##if totalCount > 100000: break

                if totalCount > 1 and (totalCount % 100000) == 0:
                    print("{:,}".format(totalCount))

            elem.clear()

elapsed_time = time.time() - start_time


print("Total pages: {:,}".format(totalCount))
print("Article pages: {:,}".format(articleCount))
print("Redirect pages: {:,}".format(redirectCount))
print("Total plain wiki pages: {:,}".format(wikiCount))
print("Elapsed time: {}".format(hms_string(elapsed_time)))

