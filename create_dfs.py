#!/usr/bin/env python
# coding: utf-8

# # Creation of main_df2
# 
# This csv contains information on the mortality related to certain diagnoses or pairs of diagnoses.

# In[3]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from itertools import combinations


# In[4]:


main_df2 = pd.read_csv('all_diagnoses.csv') # This file will be generated using SQL queries


# # Top 50 Diagnosis & Interactions

# ## Single Diagnoses

# In[5]:


deathrate = []
count_deaths_list = []

for condition in main_df2['SHORT_TITLE'].unique():
    subset = main_df2[main_df2['SHORT_TITLE']==condition]
    count = subset.shape[0]
    count_deaths = subset['HOSPITAL_EXPIRE_FLAG'].sum()
    deathrate.append(count_deaths/count)
    count_deaths_list.append(count_deaths)


# In[6]:


deathrate_df = pd.DataFrame()
deathrate_df['SHORT_TITLE'] = main_df2['SHORT_TITLE'].unique()
deathrate_df['DEATHRATE'] = deathrate
deathrate_df['PATIENT_COUNT'] = count_deaths_list
deathrate_df_sorted = deathrate_df.sort_values(by='DEATHRATE',ascending=False)
deathrate_df_sorted_top=deathrate_df_sorted[deathrate_df_sorted["PATIENT_COUNT"]>=10]


# ## Interactions

# In[7]:


def check_interactions(data,diagnoses):
    count_all = []
    count_deaths_all = []
    combo1 = []
    combo2 = []
    icu_ids = []
    for i,j in combinations(diagnoses, 2):
        count = 0
        count_deaths = 0
        print(i,j)
        icu_i = data['ICUSTAY_ID'][data['SHORT_TITLE'] == i].unique()
        icu_j = data['ICUSTAY_ID'][data['SHORT_TITLE'] == j].unique()
        both = [value for value in icu_i if value in icu_j]
        
        for icu_stay in both:
            data_subset = data[data['ICUSTAY_ID'] == icu_stay]
            count += 1
            count_deaths += [i for i in data_subset['HOSPITAL_EXPIRE_FLAG']][0] #data_subset['HOSPITAL_EXPIRE_FLAG'].to_list()[0] 
        print(count_deaths)
        print(count)
        combo1.append(i)
        combo2.append(j)
        count_all.append(count)
        count_deaths_all.append(count_deaths)
        icu_ids.append(both)
    return combo1, combo2, count_all, count_deaths_all, icu_ids


# In[8]:


dr_df = deathrate_df_sorted_top.copy()
dr_df.loc[(dr_df['DEATHRATE'] >= 0.7), 'DEATHRATE_CAT'] = 2
dr_df.loc[(dr_df['DEATHRATE'] < 0.7) & (dr_df['DEATHRATE'] >= 0.5), 'DEATHRATE_CAT'] = 1
dr_df.loc[(dr_df['DEATHRATE'] < 0.5), 'DEATHRATE_CAT'] = 0


# In[9]:


dr_df[dr_df.DEATHRATE>0.5]


# In[12]:


top50_diagn = dr_df[dr_df["PATIENT_COUNT"]>=10].head(50)['SHORT_TITLE']
combo1, combo2, count_all, count_deaths_all, icu_ids = check_interactions(main_df2,top50_diagn)


# In[13]:


interactions_df = pd.DataFrame()
interactions_df['diagnosis1']=combo1
interactions_df['diagnosis2']=combo2
interactions_df['total_count']=count_all
interactions_df['death_count']=count_deaths_all
interactions_df['icu_ids'] = icu_ids
death_rate=[]
for i,j in zip(count_deaths_all,count_all):
    if j!=0:
        death_rate.append(i/j)
    else:
        death_rate.append(0)
interactions_df['death_rate']=death_rate 
interactions_df= interactions_df[interactions_df['total_count']>=10].sort_values(by='death_rate', ascending=False)

# Join first Diagnosis with individual diagnosis deathrates
df_comp = interactions_df.merge(dr_df, how='left', left_on='diagnosis1',right_on="SHORT_TITLE")
df_comp = df_comp.drop(['SHORT_TITLE','PATIENT_COUNT','DEATHRATE_CAT'], axis=1)
df_comp = df_comp.rename(columns={"DEATHRATE": "deathrate_diagn1"})

# Join first Diagnosis with individual diagnosis deathrates
df_comp = df_comp.merge(dr_df, how='left', left_on='diagnosis2',right_on="SHORT_TITLE")
df_comp = df_comp.drop(['SHORT_TITLE','PATIENT_COUNT','DEATHRATE_CAT'], axis=1)
df_comp = df_comp.rename(columns={"DEATHRATE": "deathrate_diagn2"})

data_subset1 = df_comp[(df_comp['death_rate']) > (0.25 + df_comp['deathrate_diagn1'] ) ]
data_subset2 = df_comp[(df_comp['death_rate']) > (0.25 + df_comp['deathrate_diagn2'] ) ]
data_subset3 = pd.concat([data_subset1,data_subset2]).drop_duplicates(subset=['diagnosis1','diagnosis2','death_rate'])
data_subset3 = data_subset3.sort_values(by='death_rate',ascending=False)



# In[16]:


#Set threshold at 0.9
data_subset3.loc[data_subset3['death_rate'] >= 0.9, 'death_rate_int_cat'] = 2
data_subset3.loc[(data_subset3['death_rate'] < 0.9) & (data_subset3['death_rate'] >= 0.5), 'death_rate_int_cat'] = 1


# In[17]:


data_subset4 = data_subset3[['icu_ids','death_rate_int_cat']]
data_subset5 = pd.DataFrame(data_subset4.icu_ids.tolist(), index=data_subset4.death_rate_int_cat).stack().reset_index(level=1, drop=True).reset_index(name='icu_ids')[['icu_ids','death_rate_int_cat']]

data_subset6 = pd.concat([data_subset5.icu_ids,pd.get_dummies(data_subset5['death_rate_int_cat'], prefix='DEATHRATE_INT_CAT_',drop_first=False)],axis=1)
data_subset7 = data_subset6.groupby('icu_ids').sum()

data_subset8 = data_subset7.where(data_subset7<=1, 1)


# In[18]:


categories = pd.get_dummies(dr_df['DEATHRATE_CAT'], prefix='DEATHRATE_CAT_',drop_first=True)
conditions_encoding = pd.concat([dr_df['SHORT_TITLE'],categories], axis=1)


# In[19]:


main_df3 = main_df2.merge(conditions_encoding, how='left', on='SHORT_TITLE')


# In[20]:


collapse_categories = main_df3[['ICUSTAY_ID','DEATHRATE_CAT__1.0', 'DEATHRATE_CAT__2.0']].groupby('ICUSTAY_ID').sum() #, 'DEATHRATE_CAT__3.0','DEATHRATE_CAT__4.0'
main_df2=main_df2.drop_duplicates(subset='ICUSTAY_ID',keep='first').sort_values(by='ICUSTAY_ID')
main_df2 = main_df2.merge(collapse_categories, how='left', on='ICUSTAY_ID')


# In[21]:


main_sub = main_df2[['ICUSTAY_ID']].merge(data_subset8.reset_index(), how='left', left_on='ICUSTAY_ID', right_on='icu_ids')
main_sub = main_sub.drop('icu_ids',axis=1).fillna(0)
main_df2 = main_df2.merge(main_sub, how='left', on='ICUSTAY_ID')


# In[22]:


main_df2.to_csv('main_df2.csv', index=False)


# # Creation of df_snapshots

# In[25]:


main_df2 = pd.read_csv("main_df2.csv")
severity_scores_df = pd.read_csv("severity_scores_df.csv")
vitals_df = pd.read_csv("vitals_df.csv")


# In[26]:


severity_scores_df=severity_scores_df.drop_duplicates(subset='subject_id',keep='first').sort_values(by='subject_id')

# Removing redundant columns Subject_ID and HADM_ID
severity_scores_df = severity_scores_df.drop(['subject_id','hadm_id','icustay_expire_flag','ICUSTAY_AGE_GROUP',
                                              'OASIS', 'OASIS_PROB','age_score','preiculos_score', 'gcs_score',
                                                'heartrate_score', 'meanbp_score',
                                               'resprate_score', 'temp_score',
                                               'UrineOutput_score', 'mechvent_score',
                                               'electivesurgery_score', 'SAPS', 'resp_score', 'vent_score',
                                               'hematocrit_score', 'glucose_score', 'SAPSII', 'SAPSII_PROB',
                                               'PaO2FiO2_score', 'bilirubin_score', 'comorbidity_score',
                                               'admissiontype_score', 'SOFA','age'], axis=1)


# In[29]:


# Remove duplicate 'DEATHRATE_INT_CAT__' columns
#main_df2 = main_df2.drop(['DEATHRATE_INT_CAT__1.0_x', 'DEATHRATE_INT_CAT__2.0_x'],axis=1)
main_df2 = main_df2.rename({'DEATHRATE_INT_CAT__1.0_y':'DEATHRATE_INT_CAT__1.0',
                            'DEATHRATE_INT_CAT__2.0_y':'DEATHRATE_INT_CAT__2.0'},axis=1)


# In[33]:


df = main_df2.merge(severity_scores_df, how='left', left_on = 'ICUSTAY_ID' ,right_on='icustay_id')
df = df.merge(vitals_df, how='inner', on='ICUSTAY_ID')


# In[34]:


# Binary Encoding

from category_encoders.binary import BinaryEncoder

diagnosis_encoder = BinaryEncoder()
diagnosis_binary = diagnosis_encoder.fit_transform(df['ICD9_CODE'].astype(str))

# Create dummies for Gender
gender_df =pd.get_dummies(df ['GENDER'],prefix='gender_', drop_first=True)

# Create dummies for Age Group
age_group_df =pd.get_dummies(df ['ICUSTAY_AGE_GROUP'], prefix='age_group_', drop_first=True)

# Create dummies for Admission Type
admtype_df =pd.get_dummies(df ['ADMISSION_TYPE'],prefix='ADMISSION_TYPE_', drop_first=True)

# Create dummies for Insurance Type
instype_df =pd.get_dummies(df ['INSURANCE'],prefix='INSURANCE_TYPE_', drop_first=True)

# Creat dummies for car_unit
care_unit = pd.get_dummies(df ['CURR_CAREUNIT'], prefix='careunit_', drop_first=True)

# Merge all encoded variables
df2 = pd.concat([df,gender_df,age_group_df,admtype_df,instype_df,care_unit], axis=1) #, diagnosis_binary


# In[35]:


# Dealing with time variables
df2['OUTTIME'] = pd.to_datetime(df2['OUTTIME'],format='%Y-%m-%d')
df2['INTIME'] = pd.to_datetime(df2['INTIME'],format='%Y-%m-%d')

# Shift timescale
df2['INTIME_ACTUAL']= df2['INTIME']+ pd.to_timedelta(df2['DIFF'],unit='D')
df2['OUTTIME_ACTUAL']= df2['OUTTIME']+ pd.to_timedelta(df2['DIFF'],unit='D')


# In[41]:


main_df2 = pd.read_csv("main_df2.csv")

# Remove duplicate 'DEATHRATE_INT_CAT__' columns
#main_df2 = main_df2.drop(['DEATHRATE_INT_CAT__1.0_x', 'DEATHRATE_INT_CAT__2.0_x'],axis=1)
main_df2 = main_df2.rename({'DEATHRATE_INT_CAT__1.0_y':'DEATHRATE_INT_CAT__1.0',
                            'DEATHRATE_INT_CAT__2.0_y':'DEATHRATE_INT_CAT__2.0'},axis=1)

df = main_df2.merge(severity_scores_df, how='left', left_on = 'ICUSTAY_ID' ,right_on='icustay_id')
df = df.merge(vitals_df, how='inner', on='ICUSTAY_ID')

# Binary Encoding

from category_encoders.binary import BinaryEncoder

diagnosis_encoder = BinaryEncoder()
diagnosis_binary = diagnosis_encoder.fit_transform(df['ICD9_CODE'].astype(str))

# Create dummies for Gender
gender_df =pd.get_dummies(df ['GENDER'],prefix='gender_', drop_first=True)

# Create dummies for Age Group
age_group_df =pd.get_dummies(df ['ICUSTAY_AGE_GROUP'], prefix='age_group_', drop_first=True)

# Create dummies for Admission Type
admtype_df =pd.get_dummies(df ['ADMISSION_TYPE'],prefix='ADMISSION_TYPE_', drop_first=True)

# Create dummies for Insurance Type
instype_df =pd.get_dummies(df ['INSURANCE'],prefix='INSURANCE_TYPE_', drop_first=True)

# Creat dummies for car_unit
care_unit = pd.get_dummies(df ['CURR_CAREUNIT'], prefix='careunit_', drop_first=True)

# Merge all encoded variables
df2 = pd.concat([df,gender_df,age_group_df,admtype_df,instype_df,care_unit], axis=1) #, diagnosis_binary


# Dealing with time variables

df2['OUTTIME'] = pd.to_datetime(df2['OUTTIME'],format='%Y-%m-%d')
df2['INTIME'] = pd.to_datetime(df2['INTIME'],format='%Y-%m-%d')

# Shift timescale
df2['INTIME_ACTUAL']= df2['INTIME']+ pd.to_timedelta(df2['DIFF'],unit='D')
df2['OUTTIME_ACTUAL']= df2['OUTTIME']+ pd.to_timedelta(df2['DIFF'],unit='D')


# In[42]:


# Only takes the first three characters from ICD9_Code as the rest is only decimal points 
#(https://mimic.physionet.org/mimictables/diagnoses_icd/)
df2['ICD9_CODE'] = df2['ICD9_CODE'].apply(lambda x: x[:3])
# Converts ICD9 Codes of type "O" to integer in case string contains only digits, else None
df2['ICD9_CODE'] = df2['ICD9_CODE'].apply(lambda x: int(x) if str(x).isdigit() else None)

def f(x):
    if x['ICD9_CODE'] > 1 and x['ICD9_CODE'] < 139: return 'Infectious And Parasitic Diseases'
    elif x['ICD9_CODE'] > 140 and x['ICD9_CODE'] < 239: return 'Neoplasms'
    elif x['ICD9_CODE'] > 240 and x['ICD9_CODE'] < 279: return 'Endocrine, Nutritional And Metabolic Diseases, And Immunity Disorders'
    elif x['ICD9_CODE'] > 280 and x['ICD9_CODE'] < 289: return 'Diseases Of The Blood And Blood-Forming Organs'
    elif x['ICD9_CODE'] > 290 and x['ICD9_CODE'] < 319: return 'Mental Disorders'
    elif x['ICD9_CODE'] > 320 and x['ICD9_CODE'] < 389: return 'Diseases Of The Nervous System And Sense Organs'
    elif x['ICD9_CODE'] > 390 and x['ICD9_CODE'] < 459: return 'Diseases Of The Circulatory System'
    elif x['ICD9_CODE'] > 460 and x['ICD9_CODE'] < 519: return 'Diseases Of The Respiratory System'
    elif x['ICD9_CODE'] > 520 and x['ICD9_CODE'] < 579: return 'Diseases Of The Digestive System'
    elif x['ICD9_CODE'] > 580 and x['ICD9_CODE'] < 629: return 'Diseases Of The Genitourinary System'
    elif x['ICD9_CODE'] > 630 and x['ICD9_CODE'] < 679: return 'Complications Of Pregnancy, Childbirth, And The Puerperium'
    elif x['ICD9_CODE'] > 680 and x['ICD9_CODE'] < 709: return 'Diseases Of The Skin And Subcutaneous Tissue'
    elif x['ICD9_CODE'] > 710 and x['ICD9_CODE'] < 739: return 'Diseases Of The Musculoskeletal System And Connective Tissue'
    elif x['ICD9_CODE'] > 740 and x['ICD9_CODE'] < 759: return 'Congenital Anomalies'
    elif x['ICD9_CODE'] > 760 and x['ICD9_CODE'] < 779: return 'Certain Conditions Originating In The Perinatal Period'
    elif x['ICD9_CODE'] > 780 and x['ICD9_CODE'] < 799: return 'Symptoms, Signs, And Ill-Defined Conditions'
    elif x['ICD9_CODE'] > 800 and x['ICD9_CODE'] < 999: return 'Injury And Poisoning'
    else: return "Other"
df2['ICD9_CODE_CAT'] = df2.apply(f, axis=1)


# In[43]:


import datetime
icustay_ids = [f for f in df2['ICUSTAY_ID']]
intimes = [t.date() for t in df2['INTIME_ACTUAL']]
outtimes = [t.date() for t in df2['OUTTIME_ACTUAL']]
expire_flags = [f for f in df2['HOSPITAL_EXPIRE_FLAG']]


# In[44]:


#Add ICUSTAY_ID

df_snapshots = pd.DataFrame(columns=['icustay_id','date','days_since_admission','out_flag'])

for ids,i,o,f in zip(icustay_ids , intimes ,outtimes , expire_flags ):
    df_temp = pd.DataFrame(columns=['icustay_id','date','days_since_admission','out_flag'])
    length = int((o-i) / datetime.timedelta(days=1))+1
    date_list = [i + datetime.timedelta(days=x) for x in range(length)]
    since_admission = [x for x in range(length)]
    icustay = [ids for x in range(length)]
    flag = [0]*length #[0 for x in range(length)]
    if f==1:
        flag[-1] = 1 #death
    else:
        flag[-1] = -1 #discharge
    df_temp['icustay_id'] = [ids]*length #[ids for x in range(length)]
    df_temp['date'] = [str(i + datetime.timedelta(days=x)) for x in range(length)] #[str(t) for t in date_list]
    df_temp['days_since_admission'] = [x for x in range(length)]
    df_temp['out_flag'] = flag
    df_snapshots = pd.concat([df_snapshots,df_temp])


# In[45]:


df_snapshots.to_csv('df_snapshots.csv', index=False)


# In[ ]:




