library("bigrquery")
library(DBI)
library(dplyr)
library(data.table)

Sys.setenv(BIGQUERY_TEST_PROJECT="bgse-dsc")
billing <- bq_test_project()

con <- dbConnect(
  bigrquery::bigquery(),
  project = "bgse-dsc",
  dataset = "MIMIC3_V1_4",
  billing = billing
)

dbListTables(con)

admissions <- tbl(con, "ADMISSIONS")
admissions <- as.data.table(admissions)

patients <- tbl(con, "PATIENTS")
patients <- as.data.table(patients)

diagnoses_icd <- tbl(con, "DIAGNOSES_ICD")
diagnoses_icd <- as.data.table(diagnoses_icd)

d_icd_diagnoses <- tbl(con, "D_ICD_DIAGNOSES")
d_icd_diagnoses <- as.data.table(d_icd_diagnoses)

##### THESE QUERIES BELOW CREATE 5 OUT OF THE 7 CVSs USED FOR THE MODEL RUNS #####


sql6 <- "SELECT p.SUBJECT_ID, p.HADM_ID, p.ICUSTAY_ID, p.ITEMID, it.LABEL, 
DATE_DIFF(DATE(p.STARTTIME),DATE(adm.ADMITTIME),DAY) AS DAYSIN_START,
DATE_DIFF(DATE(p.ENDTIME),DATE(adm.ADMITTIME),DAY) AS DAYSIN_END,
FROM `bgse-dsc.MIMIC3_V1_4.PROCEDUREEVENTS_MV` p
RIGHT JOIN `bgse-dsc.MIMIC3_V1_4.ADMISSIONS` adm
USING (HADM_ID)
LEFT JOIN `bgse-dsc.MIMIC3_V1_4.D_ITEMS` it
USING (ITEMID)
WHERE ICUSTAY_ID IS NOT NULL;"


procedures_df <- dbGetQuery(con, sql6, n = 100000)
write.csv(procedures_df, "procedures.csv",row.names = FALSE)


sql8 <- "SELECT * 
  FROM `bgse-dsc.MIMIC3_V1_4.VK_vitalsfirstday` ;"

vitals_df <- dbGetQuery(con, sql8, n = 100000)
write.csv(vitals_df, "vitals_df.csv",row.names = FALSE)

sql9 <- "SELECT ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID,
    ie.INTIME, ie.OUTTIME,ie.LOS ,ie.CURR_CAREUNIT ,ie.DIFF,ie.LAST_WARDID,pat.GENDER,pat.DOB,dia.ICD9_CODE, dia2.SHORT_TITLE, dia2.LONG_TITLE,SEQ_NUM,
    adm.DEATHTIME,adm.HOSPITAL_EXPIRE_FLAG,adm.INSURANCE,adm.ADMISSION_TYPE,
    DATETIME_DIFF(adm.ADMITTIME, pat.DOB, YEAR) as age,
    CASE
        WHEN DATETIME_DIFF(adm.ADMITTIME, pat.DOB, YEAR) <= 1
            THEN 'neonate'
        WHEN DATETIME_DIFF(adm.ADMITTIME, pat.DOB, YEAR) <= 14
            THEN '1-14'
        WHEN DATETIME_DIFF(adm.ADMITTIME, pat.DOB, YEAR) <= 39
            THEN '15-39'
        WHEN DATETIME_DIFF(adm.ADMITTIME, pat.DOB, YEAR) <= 64
            THEN '40-64'
        WHEN DATETIME_DIFF(adm.ADMITTIME, pat.DOB, YEAR) <= 89
            THEN '65-89'
        WHEN DATETIME_DIFF(adm.ADMITTIME, pat.DOB, YEAR) > 89
            THEN '>89'
    END AS ICUSTAY_AGE_GROUP
FROM `bgse-dsc.MIMIC3_V1_4.ICUSTAYS_TRANS_COLLAPSED_V2` ie
INNER JOIN `bgse-dsc.MIMIC3_V1_4.PATIENTS` pat
  ON ie.SUBJECT_ID = pat.SUBJECT_ID
INNER JOIN `bgse-dsc.MIMIC3_V1_4.ADMISSIONS` adm
  ON ie.HADM_ID = adm.HADM_ID
INNER JOIN `bgse-dsc.MIMIC3_V1_4.DIAGNOSES_ICD` dia
  ON ie.HADM_ID = dia.HADM_ID
INNER JOIN `bgse-dsc.MIMIC3_V1_4.D_ICD_DIAGNOSES` dia2
  ON dia.ICD9_CODE = dia2.ICD9_CODE;"


main_df2 <- dbGetQuery(con, sql9)
write.csv(main_df2, "all_diagnoses.csv",row.names = FALSE)



sql11 <- "SELECT * 
  FROM `bgse-dsc.MIMIC3_V1_4.VK_allscores` ;"

severity_scores_df <- dbGetQuery(con, sql11)
write.csv(severity_scores_df, "severity_scores_df.csv",row.names = FALSE)


sql13 <- "SELECT 
COUNT(DISTINCT case when CLASS = 'Doctor' then ca.CGID else null end) as COUNT_DOCTORS,
COUNT(DISTINCT case when CLASS = 'Nurse' then ca.CGID else null end) as COUNT_NURSES, 
COUNT(DISTINCT ch.SUBJECT_ID) AS COUNT_PATIENTS,
DATE_ADD(DATE(ch.CHARTTIME), interval CAST(icu.Diff AS INT64) Day) AS DATE
FROM `bgse-dsc.MIMIC3_V1_4.CHARTEVENTS` ch 
LEFT JOIN  `bgse-dsc.MIMIC3_V1_4.CAREGIVERS_EXTENDED` ca
  ON ch.CGID = ca.CGID
LEFT JOIN  `bgse-dsc.MIMIC3_V1_4.ICUSTAYS_TRANS_COLLAPSED_V2` icu
  ON ch.ICUSTAY_ID = icu.ICUSTAY_ID
GROUP BY DATE;"

caregivers_count_df <- dbGetQuery(con, sql13)
write.csv(caregivers_count_df, "caregivers_count_df.csv",row.names = FALSE)

