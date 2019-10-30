#!/usr/bin/env python
# coding: utf-8

# In[2]:


import csv
from datetime import datetime, timedelta
import pyodbc


# In[60]:


conn = pyodbc.connect('DSN=kubricksql;UID=DE14;PWD=password')
cur = conn.cursor()


# In[61]:


sharkfile = r'c:\data\GSAF5.csv'


# In[62]:


attack_dates = []
case = []
activity = []
age = []
gender = []
country = []
isfatal = []
with open(sharkfile) as f:
    reader = csv.DictReader(f)
    #reader = csv.reader(f)
    for row in reader:
        case.append(row['Case Number'])
        attack_dates.append(row['Date'])
        country.append(row['Country'])
        activity.append(row['Activity'])
        age.append(row['Age'])
        gender.append(row['Sex '])
        isfatal.append(row['Fatal (Y/N)'])


# In[63]:


data = zip(attack_dates, case, country, activity, age, gender, isfatal)


# In[64]:


cur.execute('truncate table lawrence.shark')


# In[65]:


q = 'insert into lawrence.shark(attack_date, case_number, country, activity, age, gender, isfatal) values (?, ?, ?, ?, ?, ?, ?)'


# In[66]:


for d in data:
    try:
        cur.execute(q, d)
        conn.commit()
    except:
        conn.rollback()


# In[ ]:




