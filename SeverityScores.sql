-- This code creates tables in BigQuery with information regarding Severity Scores and the vital signs that are used in their calculations


-- allvitals table

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_vitalsall` AS
SELECT main.* EXCEPT (item_date), DATE_DIFF(main.item_date, DATE(adm.ADMITTIME), DAY) AS days_since_admission FROM 
(SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.item_date
-- Easier names
, min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
, max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
, avg(case when VitalID = 1 then valuenum else null end) as HeartRate_Mean
, min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
, max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
, avg(case when VitalID = 2 then valuenum else null end) as SysBP_Mean
, min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
, max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
, avg(case when VitalID = 3 then valuenum else null end) as DiasBP_Mean
, min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
, max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
, avg(case when VitalID = 4 then valuenum else null end) as MeanBP_Mean
, min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
, max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
, avg(case when VitalID = 5 then valuenum else null end) as RespRate_Mean
, min(case when VitalID = 6 then valuenum else null end) as TempC_Min
, max(case when VitalID = 6 then valuenum else null end) as TempC_Max
, avg(case when VitalID = 6 then valuenum else null end) as TempC_Mean
, min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
, max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
, avg(case when VitalID = 7 then valuenum else null end) as SpO2_Mean
, min(case when VitalID = 8 then valuenum else null end) as Glucose_Min
, max(case when VitalID = 8 then valuenum else null end) as Glucose_Max
, avg(case when VitalID = 8 then valuenum else null end) as Glucose_Mean
FROM  (
select ie.subject_id, ie.hadm_id, ie.icustay_id, DATE(ce.CHARTTIME) AS item_date
, case
when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- HeartRate
when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- SysBP
when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- DiasBP
when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- MeanBP
when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5 -- RespRate
when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- TempF, converted to degC in valuenum call
when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 -- TempC
when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7 -- SpO2
when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8 -- Glucose
else null end as VitalID
-- convert F to C
, case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum
from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS_TRANS_COLLAPSED_V2` ie
left join `bgse-dsc.MIMIC3_V1_4.CHARTEVENTS` ce
on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
and DATE(ce.CHARTTIME) between DATE(ie.INTIME) and DATE_ADD(DATE(ie.INTIME), interval 2 DAY)
and ce.error != 1
where ce.itemid in
(
  -- HEART RATE
  211, 
  220045,
  -- Systolic/diastolic
  51, 
  442, 
  455, 
  6701,
  220179, 
  220050, 
  8368, 
  8440, 
  8441, 
  8555,
  220180, 
  220051, 
  -- MEAN ARTERIAL PRESSURE
  456, 
  52, 
  6702, 
  443, 
  220052,  
  220181, 
  225312, 
  -- RESPIRATORY RATE
  618,
  615,
  220210,
  224690, 
  -- SPO2, peripheral
  646, 220277,
  -- GLUCOSE, both lab and fingerstick
  807,
  811,
  1529,
  3745,
  3744,
  225664,
  220621,
  226537,
  -- TEMPERATURE
  223762, 
  676,	
  223761, 
  678 
)
) pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.item_date
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id) main
left join `bgse-dsc.MIMIC3_V1_4.ADMISSIONS` adm
USING (HADM_ID)
ORDER BY main.subject_id, main.hadm_id, main.icustay_id, main.item_date
;



-- OASIS
CREATE TABLE `bgse-dsc.MIMIC3_V1_4.OASIS` as

with surgflag as
(
  select ie.icustay_id
    , max(case
        when lower(curr_service) like '%surg%' then 1
        when curr_service = 'ORTHO' then 1
    else 0 end) as surgical
  from icustays ie
  left join services se
    on ie.hadm_id = se.hadm_id
    and DATE_ADD(se.transfertime < ie.intime, interval 1 day)
  group by ie.icustay_id
)
, cohort as
(
select ie.subject_id, ie.hadm_id, ie.icustay_id
      , ie.intime
      , ie.outtime
      , adm.deathtime
      , cast(ie.intime as timestamp) - cast(adm.admittime as timestamp) as PreICULOS
      , floor( ( cast(ie.intime as date) - cast(pat.dob as date) ) / 365.242 ) as age
      , gcs.mingcs
      , vital.heartrate_max
      , vital.heartrate_min
      , vital.meanbp_max
      , vital.meanbp_min
      , vital.resprate_max
      , vital.resprate_min
      , vital.tempc_max
      , vital.tempc_min
      , vent.vent as mechvent
      , uo.urineoutput

      , case
          when adm.ADMISSION_TYPE = 'ELECTIVE' and sf.surgical = 1
            then 1
          when adm.ADMISSION_TYPE is null or sf.surgical is null
            then null
          else 0
        end as ElectiveSurgery

      -- age group
      , case
        when ( ( cast(ie.intime as date) - cast(pat.dob as date) ) / 365.242 ) <= 1 then 'neonate'
        when ( ( cast(ie.intime as date) - cast(pat.dob as date) ) / 365.242 ) <= 15 then 'middle'
        else 'adult' end as ICUSTAY_AGE_GROUP

      -- mortality flags
      , case
          when adm.deathtime between ie.intime and ie.outtime
            then 1
          when adm.deathtime <= ie.intime -- sometimes there are typographical errors in the death date
            then 1
          when adm.dischtime <= ie.outtime and adm.discharge_location = 'DEAD/EXPIRED'
            then 1
          else 0 end
        as ICUSTAY_EXPIRE_FLAG
      , adm.hospital_expire_flag
from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
inner join `bgse-dsc.MIMIC3_V1_4.ADMISSIONS` adm
  on ie.hadm_id = adm.hadm_id
inner join `bgse-dsc.MIMIC3_V1_4.PATIENTS` pat
  on ie.subject_id = pat.subject_id
left join surgflag sf
  on ie.icustay_id = sf.icustay_id
-- join to custom tables to get more data....
left join `bgse-dsc.MIMIC3_V1_4.VK_gcsfirstday` gcs
  on ie.icustay_id = gcs.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_vitalsfirstday` vital
  on ie.icustay_id = vital.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_uofirstday` uo
  on ie.icustay_id = uo.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_ventfirstday` vent
  on ie.icustay_id = vent.icustay_id
)
, scorecomp as
(
select co.subject_id, co.hadm_id, co.icustay_id
, co.ICUSTAY_AGE_GROUP
, co.icustay_expire_flag
, co.hospital_expire_flag

-- Below code calculates the component scores needed for OASIS
, case when preiculos is null then null
     when preiculos < '0 0:10:12' then 5
     when preiculos < '0 4:57:00' then 3
     when preiculos < '1 0:00:00' then 0
     when preiculos < '12 23:48:00' then 1
     else 2 end as preiculos_score
,  case when age is null then null
      when age < 24 then 0
      when age <= 53 then 3
      when age <= 77 then 6
      when age <= 89 then 9
      when age >= 90 then 7
      else 0 end as age_score
,  case when mingcs is null then null
      when mingcs <= 7 then 10
      when mingcs < 14 then 4
      when mingcs = 14 then 3
      else 0 end as gcs_score
,  case when heartrate_max is null then null
      when heartrate_max > 125 then 6
      when heartrate_min < 33 then 4
      when heartrate_max >= 107 and heartrate_max <= 125 then 3
      when heartrate_max >= 89 and heartrate_max <= 106 then 1
      else 0 end as heartrate_score
,  case when meanbp_min is null then null
      when meanbp_min < 20.65 then 4
      when meanbp_min < 51 then 3
      when meanbp_max > 143.44 then 3
      when meanbp_min >= 51 and meanbp_min < 61.33 then 2
      else 0 end as meanbp_score
,  case when resprate_min is null then null
      when resprate_min <   6 then 10
      when resprate_max >  44 then  9
      when resprate_max >  30 then  6
      when resprate_max >  22 then  1
      when resprate_min <  13 then 1 else 0
      end as resprate_score
,  case when tempc_max is null then null
      when tempc_max > 39.88 then 6
      when tempc_min >= 33.22 and tempc_min <= 35.93 then 4
      when tempc_max >= 33.22 and tempc_max <= 35.93 then 4
      when tempc_min < 33.22 then 3
      when tempc_min > 35.93 and tempc_min <= 36.39 then 2
      when tempc_max >= 36.89 and tempc_max <= 39.88 then 2
      else 0 end as temp_score
,  case when UrineOutput is null then null
      when UrineOutput < 671.09 then 10
      when UrineOutput > 6896.80 then 8
      when UrineOutput >= 671.09
       and UrineOutput <= 1426.99 then 5
      when UrineOutput >= 1427.00
       and UrineOutput <= 2544.14 then 1
      else 0 end as UrineOutput_score
,  case when mechvent is null then null
      when mechvent = 1 then 9
      else 0 end as mechvent_score
,  case when ElectiveSurgery is null then null
      when ElectiveSurgery = 1 then 0
      else 6 end as electivesurgery_score


-- The below code gives the component associated with each score
-- This is not needed to calculate OASIS, but provided for user convenience.
-- If both the min/max are in the normal range (score of 0), then the average value is stored.
, preiculos
, age
, mingcs as gcs
,  case when heartrate_max is null then null
      when heartrate_max > 125 then heartrate_max
      when heartrate_min < 33 then heartrate_min
      when heartrate_max >= 107 and heartrate_max <= 125 then heartrate_max
      when heartrate_max >= 89 and heartrate_max <= 106 then heartrate_max
      else (heartrate_min+heartrate_max)/2 end as heartrate
,  case when meanbp_min is null then null
      when meanbp_min < 20.65 then meanbp_min
      when meanbp_min < 51 then meanbp_min
      when meanbp_max > 143.44 then meanbp_max
      when meanbp_min >= 51 and meanbp_min < 61.33 then meanbp_min
      else (meanbp_min+meanbp_max)/2 end as meanbp
,  case when resprate_min is null then null
      when resprate_min <   6 then resprate_min
      when resprate_max >  44 then resprate_max
      when resprate_max >  30 then resprate_max
      when resprate_max >  22 then resprate_max
      when resprate_min <  13 then resprate_min
      else (resprate_min+resprate_max)/2 end as resprate
,  case when tempc_max is null then null
      when tempc_max > 39.88 then tempc_max
      when tempc_min >= 33.22 and tempc_min <= 35.93 then tempc_min
      when tempc_max >= 33.22 and tempc_max <= 35.93 then tempc_max
      when tempc_min < 33.22 then tempc_min
      when tempc_min > 35.93 and tempc_min <= 36.39 then tempc_min
      when tempc_max >= 36.89 and tempc_max <= 39.88 then tempc_max
      else (tempc_min+tempc_max)/2 end as temp
,  UrineOutput
,  mechvent
,  ElectiveSurgery
from cohort co
)
, score as
(
select s.*
    , coalesce(age_score,0)
    + coalesce(preiculos_score,0)
    + coalesce(gcs_score,0)
    + coalesce(heartrate_score,0)
    + coalesce(meanbp_score,0)
    + coalesce(resprate_score,0)
    + coalesce(temp_score,0)
    + coalesce(urineoutput_score,0)
    + coalesce(mechvent_score,0)
    + coalesce(electivesurgery_score,0)
    as OASIS
from scorecomp s
)
select
  subject_id, hadm_id, icustay_id
  , ICUSTAY_AGE_GROUP
  , hospital_expire_flag
  , icustay_expire_flag
  , OASIS
  -- Calculate the probability of in-hospital mortality
  , 1 / (1 + exp(- (-6.1746 + 0.1275*(OASIS) ))) as OASIS_PROB
  , age, age_score
  , preiculos, preiculos_score
  , gcs, gcs_score
  , heartrate, heartrate_score
  , meanbp, meanbp_score
  , resprate, resprate_score
  , temp, temp_score
  , urineoutput, UrineOutput_score
  , mechvent, mechvent_score
  , electivesurgery, electivesurgery_score
from score
order by icustay_id;



-- SAPS


CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_SAPS` as
-- extract CPAP from the "Oxygen Delivery Device" fields
with cpap as
(
  select ie.icustay_id
    , max(case when lower(value) like '%(cpap mask|bipap mask)%' then 1 else 0 end) as cpap
  from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
  inner join `bgse-dsc.MIMIC3_V1_4.CHARTEVENTS` ce
    on ie.icustay_id = ce.icustay_id
    and DATE(ce.charttime) between DATE(ie.intime) and DATE_ADD(DATE(ie.intime), interval 1 day)
  where itemid in
  (
    -- TODO: when metavision data import fixed, check the values in 226732 match the value clause below
    467, 469, 226732
  )
  and lower(ce.value) like '%(cpap mask|bipap mask)%'
  -- exclude rows marked as error
  AND ce.error != 1
  group by ie.icustay_id
)
, cohort as
(
select ie.subject_id, ie.hadm_id, ie.icustay_id
      , ie.intime
      , ie.outtime

      -- the casts ensure the result is numeric.. we could equally extract EPOCH from the interval
      -- however this code works in Oracle and Postgres
      , round( DATE_DIFF( cast(ie.intime as date), cast(pat.dob as date), DAY ) / 365.242 , 2 ) as age
      , gcs.mingcs
      , vital.heartrate_max
      , vital.heartrate_min
      , vital.sysbp_max
      , vital.sysbp_min
      , vital.resprate_max
      , vital.resprate_min
      , vital.tempc_max
      , vital.tempc_min

      , coalesce(vital.glucose_max, labs.glucose_max) as glucose_max
      , coalesce(vital.glucose_min, labs.glucose_min) as glucose_min

      , labs.bun_max
      , labs.bun_min
      , labs.hematocrit_max
      , labs.hematocrit_min
      , labs.wbc_max
      , labs.wbc_min
      , labs.sodium_max
      , labs.sodium_min
      , labs.potassium_max
      , labs.potassium_min
      , labs.bicarbonate_max
      , labs.bicarbonate_min

      , vent.vent as mechvent
      , uo.urineoutput

      , cp.cpap

from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
inner join `bgse-dsc.MIMIC3_V1_4.ADMISSIONS` adm
  on ie.hadm_id = adm.hadm_id
inner join `bgse-dsc.MIMIC3_V1_4.PATIENTS` pat
  on ie.subject_id = pat.subject_id

-- join to above view to get CPAP
left join cpap cp
  on ie.icustay_id = cp.icustay_id

-- join to custom tables to get more data....
left join `bgse-dsc.MIMIC3_V1_4.VK_gcsfirstday` gcs
  on ie.icustay_id = gcs.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_vitalsfirstday` vital
  on ie.icustay_id = vital.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_uofirstday` uo
  on ie.icustay_id = uo.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_ventfirstday` vent
  on ie.icustay_id = vent.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_labsfirstday` labs
  on ie.icustay_id = labs.icustay_id
)
, scorecomp as
(
select
  cohort.*
  -- Below code calculates the component scores needed for SAPS
  , case
      when age is null then null
      when age <= 45 then 0
      when age <= 55 then 1
      when age <= 65 then 2
      when age <= 75 then 3
      when age >  75 then 4
    end as age_score
  , case
      when heartrate_max is null then null
      when heartrate_max >= 180 then 4
      when heartrate_min < 40 then 4
      when heartrate_max >= 140 then 3
      when heartrate_min <= 54 then 3
      when heartrate_max >= 110 then 2
      when heartrate_min <= 69 then 2
      when heartrate_max >= 70 and heartrate_max <= 109
        and heartrate_min >= 70 and heartrate_min <= 109
      then 0
    end as hr_score
  , case
      when sysbp_min is null then null
      when sysbp_max >= 190 then 4
      when sysbp_min < 55 then 4
      when sysbp_max >= 150 then 2
      when sysbp_min <= 79 then 2
      when sysbp_max >= 80 and sysbp_max <= 149
        and sysbp_min >= 80 and sysbp_min <= 149
        then 0
    end as sysbp_score

  , case
      when tempc_max is null then null
      when tempc_max >= 41.0 then 4
      when tempc_min <  30.0 then 4
      when tempc_max >= 39.0 then 3
      when tempc_min <= 31.9  then 3
      when tempc_min <= 33.9  then 2
      when tempc_max >  38.4 then 1
      when tempc_min <  36.0  then 1
      when tempc_max >= 36.0 and tempc_max <= 38.4
       and tempc_min >= 36.0 and tempc_min <= 38.4
        then 0
    end as temp_score

  , case
      when resprate_min is null then null
      when resprate_max >= 50 then 4
      when resprate_min <  6 then 4
      when resprate_max >= 35 then 3
      when resprate_min <= 9 then 2
      when resprate_max >= 25 then 1
      when resprate_min <= 11 then 1
      when  resprate_max >= 12 and resprate_max <= 24
        and resprate_min >= 12 and resprate_min <= 24
          then 0
      end as resp_score

  , case
      when coalesce(mechvent,cpap) is null then null
      when cpap = 1 then 3
      when mechvent = 1 then 3
      else 0
    end as vent_score

  , case
      when UrineOutput is null then null
      when UrineOutput >  5000.0 then 2
      when UrineOutput >= 3500.0 then 1
      when UrineOutput >=  700.0 then 0
      when UrineOutput >=  500.0 then 2
      when UrineOutput >=  200.0 then 3
      when UrineOutput <   200.0 then 4
    end as uo_score

  , case
      when bun_max is null then null
      when bun_max >= 55.0 then 4
      when bun_max >= 36.0 then 3
      when bun_max >= 29.0 then 2
      when bun_max >= 7.50 then 1
      when bun_min < 3.5 then 1
      when  bun_max >= 3.5 and bun_max < 7.5
        and bun_min >= 3.5 and bun_min < 7.5
          then 0
    end as bun_score

  , case
      when hematocrit_max is null then null
      when hematocrit_max >= 60.0 then 4
      when hematocrit_min <  20.0 then 4
      when hematocrit_max >= 50.0 then 2
      when hematocrit_min < 30.0 then 2
      when hematocrit_max >= 46.0 then 1
      when  hematocrit_max >= 30.0 and hematocrit_max < 46.0
        and hematocrit_min >= 30.0 and hematocrit_min < 46.0
          then 0
      end as hematocrit_score

  , case
      when wbc_max is null then null
      when wbc_max >= 40.0 then 4
      when wbc_min <   1.0 then 4
      when wbc_max >= 20.0 then 2
      when wbc_min <   3.0 then 2
      when wbc_max >= 15.0 then 1
      when wbc_max >=  3.0 and wbc_max < 15.0
       and wbc_min >=  3.0 and wbc_min < 15.0
        then 0
    end as wbc_score

  , case
      when glucose_max is null then null
      when glucose_max >= 44.5 then 4
      when glucose_min <   1.6 then 4
      when glucose_max >= 27.8 then 3
      when glucose_min <   2.8 then 3
      when glucose_min <   3.9 then 2
      when glucose_max >= 14.0 then 1
      when glucose_max >=  3.9 and glucose_max < 14.0
       and glucose_min >=  3.9 and glucose_min < 14.0
        then 0
      end as glucose_score

  , case
      when potassium_max is null then null
      when potassium_max >= 7.0 then 4
      when potassium_min <  2.5 then 4
      when potassium_max >= 6.0 then 3
      when potassium_min <  3.0 then 2
      when potassium_max >= 5.5 then 1
      when potassium_min <  3.5 then 1
      when potassium_max >= 3.5 and potassium_max < 5.5
       and potassium_min >= 3.5 and potassium_min < 5.5
        then 0
      end as potassium_score

  , case
      when sodium_max is null then null
      when sodium_max >= 180 then 4
      when sodium_min  < 110 then 4
      when sodium_max >= 161 then 3
      when sodium_min  < 120 then 3
      when sodium_max >= 156 then 2
      when sodium_min  < 130 then 2
      when sodium_max >= 151 then 1
      when sodium_max >= 130 and sodium_max < 151
       and sodium_min >= 130 and sodium_min < 151
        then 0
      end as sodium_score

  , case
      when bicarbonate_max is null then null
      when bicarbonate_min <   5.0 then 4
      when bicarbonate_max >= 40.0 then 3
      when bicarbonate_min <  10.0 then 3
      when bicarbonate_max >= 30.0 then 1
      when bicarbonate_min <  20.0 then 1
      when bicarbonate_max >= 20.0 and bicarbonate_max < 30.0
       and bicarbonate_min >= 20.0 and bicarbonate_min < 30.0
          then 0
      end as bicarbonate_score

   , case
      when mingcs is null then null
        when mingcs <  3 then null -- erroneous value/on trach
        when mingcs =  3 then 4
        when mingcs <  7 then 3
        when mingcs < 10 then 2
        when mingcs < 13 then 1
        when mingcs >= 13
         and mingcs <= 15
          then 0
        end as gcs_score
from cohort
)
select ie.subject_id, ie.hadm_id, ie.icustay_id
-- coalesce statements impute normal score of zero if data element is missing
, coalesce(age_score,0)
+ coalesce(hr_score,0)
+ coalesce(sysbp_score,0)
+ coalesce(resp_score,0)
+ coalesce(temp_score,0)
+ coalesce(uo_score,0)
+ coalesce(vent_score,0)
+ coalesce(bun_score,0)
+ coalesce(hematocrit_score,0)
+ coalesce(wbc_score,0)
+ coalesce(glucose_score,0)
+ coalesce(potassium_score,0)
+ coalesce(sodium_score,0)
+ coalesce(bicarbonate_score,0)
+ coalesce(gcs_score,0)
  as SAPS
, age_score
, hr_score
, sysbp_score
, resp_score
, temp_score
, uo_score
, vent_score
, bun_score
, hematocrit_score
, wbc_score
, glucose_score
, potassium_score
, sodium_score
, bicarbonate_score
, gcs_score

from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
left join scorecomp s
  on ie.icustay_id = s.icustay_id
order by ie.icustay_id;


-- SAPS II

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_SAPSII` as
-- extract CPAP from the "Oxygen Delivery Device" fields
with cpap as
(
  select ie.icustay_id
    , min(TIMESTAMP_ADD(TIMESTAMP(charttime), interval -1 hour)) as starttime
    , max(TIMESTAMP_ADD(TIMESTAMP(charttime), interval 4 hour)) as endtime
    , max(case when lower(ce.value) LIKE '%(cpap mask|bipap mask)%' then 1 else 0 end) as cpap
  from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
  inner join `bgse-dsc.MIMIC3_V1_4.CHARTEVENTS` ce
    on ie.icustay_id = ce.icustay_id
    and DATE(ce.charttime) between DATE(ie.intime) and DATE_ADD(DATE(ie.intime), interval 1 day)
  where itemid in
  (
    -- TODO: when metavision data import fixed, check the values in 226732 match the value clause below
    467, 469, 226732
  )
  and lower(ce.value) LIKE '%(cpap mask|bipap mask)%'
  -- exclude rows marked as error
  AND ce.error != 1
  group by ie.icustay_id
)
-- extract a flag for surgical service
-- this combined with "elective" from admissions table defines elective/non-elective surgery
, surgflag as
(
  select adm.hadm_id
    , case when lower(curr_service) like '%surg%' then 1 else 0 end as surgical
    , ROW_NUMBER() over
    (
      PARTITION BY adm.HADM_ID
      ORDER BY TRANSFERTIME
    ) as serviceOrder
  from `bgse-dsc.MIMIC3_V1_4.ADMISSIONS` adm
  left join `bgse-dsc.MIMIC3_V1_4.SERVICES` se
    on adm.hadm_id = se.hadm_id
)
-- icd-9 diagnostic codes are our best source for comorbidity information
-- unfortunately, they are technically a-causal
-- however, this shouldn't matter too much for the SAPS II comorbidities
, comorb as
(
select hadm_id
-- these are slightly different than elixhauser comorbidities, but based on them
-- they include some non-comorbid ICD-9 codes (e.g. 20302, relapse of multiple myeloma)
  , max(CASE
    when icd9_code between '042  ' and '0449 ' then 1
  		end) as AIDS      /* HIV and AIDS */
  , max(CASE
    when icd9_code between '20000' and '20238' then 1 -- lymphoma
    when icd9_code between '20240' and '20248' then 1 -- leukemia
    when icd9_code between '20250' and '20302' then 1 -- lymphoma
    when icd9_code between '20310' and '20312' then 1 -- leukemia
    when icd9_code between '20302' and '20382' then 1 -- lymphoma
    when icd9_code between '20400' and '20522' then 1 -- chronic leukemia
    when icd9_code between '20580' and '20702' then 1 -- other myeloid leukemia
    when icd9_code between '20720' and '20892' then 1 -- other myeloid leukemia
    when icd9_code = '2386 ' then 1 -- lymphoma
    when icd9_code = '2733 ' then 1 -- lymphoma
  		end) as HEM
  , max(CASE
    when icd9_code between '1960 ' and '1991 ' then 1
    when icd9_code between '20970' and '20975' then 1
    when icd9_code = '20979' then 1
    when icd9_code = '78951' then 1
  		end) as METS      /* Metastatic cancer */
  from
  (
    select hadm_id, seq_num
    , cast(icd9_code as string) as icd9_code
    from `bgse-dsc.MIMIC3_V1_4.DIAGNOSES_ICD`
  ) icd
  group by hadm_id
)
, pafi1 as
(
  -- join blood gas to ventilation durations to determine if patient was vent
  -- also join to cpap table for the same purpose
  select bg.icustay_id, bg.charttime
  , PaO2FiO2
  , case when vd.icustay_id is not null then 1 else 0 end as vent
  , case when cp.icustay_id is not null then 1 else 0 end as cpap
  from `bgse-dsc.MIMIC3_V1_4.VK_bloodgasfirstdayarterial` bg
  left join `bgse-dsc.MIMIC3_V1_4.VK_ventdurations` vd
    on bg.icustay_id = vd.icustay_id
    and bg.charttime >= vd.starttime
    and bg.charttime <= vd.endtime
  left join cpap cp
    on bg.icustay_id = cp.icustay_id
    and TIMESTAMP(bg.charttime) >= cp.starttime
    and TIMESTAMP(bg.charttime) <= cp.endtime
)
, pafi2 as
(
  -- get the minimum PaO2/FiO2 ratio *only for ventilated/cpap patients*
  select icustay_id
  , min(PaO2FiO2) as PaO2FiO2_vent_min
  from pafi1
  where vent = 1 or cpap = 1
  group by icustay_id
)
, cohort as
(
select ie.subject_id, ie.hadm_id, ie.icustay_id
      , ie.intime
      , ie.outtime

      -- the casts ensure the result is numeric.. we could equally extract EPOCH from the interval
      -- however this code works in Oracle and Postgres
      , round( DATE_DIFF( cast(ie.intime as date) , cast(pat.dob as date),DAY ) / 365.242 , 2 ) as age

      , vital.heartrate_max
      , vital.heartrate_min
      , vital.sysbp_max
      , vital.sysbp_min
      , vital.tempc_max
      , vital.tempc_min

      -- this value is non-null iff the patient is on vent/cpap
      , pf.PaO2FiO2_vent_min

      , uo.urineoutput

      , labs.bun_min
      , labs.bun_max
      , labs.wbc_min
      , labs.wbc_max
      , labs.potassium_min
      , labs.potassium_max
      , labs.sodium_min
      , labs.sodium_max
      , labs.bicarbonate_min
      , labs.bicarbonate_max
      , labs.bilirubin_min
      , labs.bilirubin_max

      , gcs.mingcs

      , comorb.AIDS
      , comorb.HEM
      , comorb.METS

      , case
          when adm.ADMISSION_TYPE = 'ELECTIVE' and sf.surgical = 1
            then 'ScheduledSurgical'
          when adm.ADMISSION_TYPE != 'ELECTIVE' and sf.surgical = 1
            then 'UnscheduledSurgical'
          else 'Medical'
        end as AdmissionType


from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
inner join `bgse-dsc.MIMIC3_V1_4.ADMISSIONS` adm
  on ie.hadm_id = adm.hadm_id
inner join `bgse-dsc.MIMIC3_V1_4.PATIENTS` pat
  on ie.subject_id = pat.subject_id

-- join to above views
left join pafi2 pf
  on ie.icustay_id = pf.icustay_id
left join surgflag sf
  on adm.hadm_id = sf.hadm_id and sf.serviceOrder = 1
left join comorb
  on ie.hadm_id = comorb.hadm_id

-- join to custom tables to get more data....
left join `bgse-dsc.MIMIC3_V1_4.VK_gcsfirstday` gcs
  on ie.icustay_id = gcs.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_vitalsfirstday` vital
  on ie.icustay_id = vital.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_uofirstday` uo
  on ie.icustay_id = uo.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_labsfirstday` labs
  on ie.icustay_id = labs.icustay_id
)
, scorecomp as
(
select
  cohort.*
  -- Below code calculates the component scores needed for SAPS
  , case
      when age is null then null
      when age <  40 then 0
      when age <  60 then 7
      when age <  70 then 12
      when age <  75 then 15
      when age <  80 then 16
      when age >= 80 then 18
    end as age_score

  , case
      when heartrate_max is null then null
      when heartrate_min <   40 then 11
      when heartrate_max >= 160 then 7
      when heartrate_max >= 120 then 4
      when heartrate_min  <  70 then 2
      when  heartrate_max >= 70 and heartrate_max < 120
        and heartrate_min >= 70 and heartrate_min < 120
      then 0
    end as hr_score

  , case
      when  sysbp_min is null then null
      when  sysbp_min <   70 then 13
      when  sysbp_min <  100 then 5
      when  sysbp_max >= 200 then 2
      when  sysbp_max >= 100 and sysbp_max < 200
        and sysbp_min >= 100 and sysbp_min < 200
        then 0
    end as sysbp_score

  , case
      when tempc_max is null then null
      when tempc_min <  39.0 then 0
      when tempc_max >= 39.0 then 3
    end as temp_score

  , case
      when PaO2FiO2_vent_min is null then null
      when PaO2FiO2_vent_min <  100 then 11
      when PaO2FiO2_vent_min <  200 then 9
      when PaO2FiO2_vent_min >= 200 then 6
    end as PaO2FiO2_score

  , case
      when UrineOutput is null then null
      when UrineOutput <   500.0 then 11
      when UrineOutput <  1000.0 then 4
      when UrineOutput >= 1000.0 then 0
    end as uo_score

  , case
      when bun_max is null then null
      when bun_max <  28.0 then 0
      when bun_max <  84.0 then 6
      when bun_max >= 84.0 then 10
    end as bun_score

  , case
      when wbc_max is null then null
      when wbc_min <   1.0 then 12
      when wbc_max >= 20.0 then 3
      when wbc_max >=  1.0 and wbc_max < 20.0
       and wbc_min >=  1.0 and wbc_min < 20.0
        then 0
    end as wbc_score

  , case
      when potassium_max is null then null
      when potassium_min <  3.0 then 3
      when potassium_max >= 5.0 then 3
      when potassium_max >= 3.0 and potassium_max < 5.0
       and potassium_min >= 3.0 and potassium_min < 5.0
        then 0
      end as potassium_score

  , case
      when sodium_max is null then null
      when sodium_min  < 125 then 5
      when sodium_max >= 145 then 1
      when sodium_max >= 125 and sodium_max < 145
       and sodium_min >= 125 and sodium_min < 145
        then 0
      end as sodium_score

  , case
      when bicarbonate_max is null then null
      when bicarbonate_min <  15.0 then 5
      when bicarbonate_min <  20.0 then 3
      when bicarbonate_max >= 20.0
       and bicarbonate_min >= 20.0
          then 0
      end as bicarbonate_score

  , case
      when bilirubin_max is null then null
      when bilirubin_max  < 4.0 then 0
      when bilirubin_max  < 6.0 then 4
      when bilirubin_max >= 6.0 then 9
      end as bilirubin_score

   , case
      when mingcs is null then null
        when mingcs <  3 then null -- erroneous value/on trach
        when mingcs <  6 then 26
        when mingcs <  9 then 13
        when mingcs < 11 then 7
        when mingcs < 14 then 5
        when mingcs >= 14
         and mingcs <= 15
          then 0
        end as gcs_score

    , case
        when AIDS = 1 then 17
        when HEM  = 1 then 10
        when METS = 1 then 9
        else 0
      end as comorbidity_score

    , case
        when AdmissionType = 'ScheduledSurgical' then 0
        when AdmissionType = 'Medical' then 6
        when AdmissionType = 'UnscheduledSurgical' then 8
        else null
      end as admissiontype_score

from cohort
)
-- Calculate SAPS II here so we can use it in the probability calculation below
, score as
(
  select s.*
  -- coalesce statements impute normal score of zero if data element is missing
  , coalesce(age_score,0)
  + coalesce(hr_score,0)
  + coalesce(sysbp_score,0)
  + coalesce(temp_score,0)
  + coalesce(PaO2FiO2_score,0)
  + coalesce(uo_score,0)
  + coalesce(bun_score,0)
  + coalesce(wbc_score,0)
  + coalesce(potassium_score,0)
  + coalesce(sodium_score,0)
  + coalesce(bicarbonate_score,0)
  + coalesce(bilirubin_score,0)
  + coalesce(gcs_score,0)
  + coalesce(comorbidity_score,0)
  + coalesce(admissiontype_score,0)
    as SAPSII
  from scorecomp s
)
select ie.subject_id, ie.hadm_id, ie.icustay_id
, SAPSII
, 1 / (1 + exp(- (-7.7631 + 0.0737*(SAPSII) + 0.9971*(ln(SAPSII + 1))) )) as SAPSII_PROB
, age_score
, hr_score
, sysbp_score
, temp_score
, PaO2FiO2_score
, uo_score
, bun_score
, wbc_score
, potassium_score
, sodium_score
, bicarbonate_score
, bilirubin_score
, gcs_score
, comorbidity_score
, admissiontype_score
from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
left join score s
  on ie.icustay_id = s.icustay_id
order by ie.icustay_id;


-- SOFA

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.SOFA` AS
with wt AS
(
  SELECT ie.icustay_id
    -- ensure weight is measured in kg
    , avg(CASE
        WHEN itemid IN (762, 763, 3723, 3580, 226512)
          THEN valuenum
        -- convert lbs to kgs
        WHEN itemid IN (3581)
          THEN valuenum * 0.45359237
        WHEN itemid IN (3582)
          THEN valuenum * 0.0283495231
        ELSE null
      END) AS weight

  from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
  left join `bgse-dsc.MIMIC3_V1_4.CHARTEVENTS` c
    on ie.icustay_id = c.icustay_id
  WHERE valuenum IS NOT NULL
  AND itemid IN
  (
    762, 763, 3723, 3580,                     -- Weight Kg
    3581,                                     -- Weight lb
    3582,                                     -- Weight oz
    226512 -- Metavision: Admission Weight (Kg)
  )
  AND valuenum != 0
  and DATE(charttime) between DATE_ADD(DATE(ie.intime), interval -1 day) and DATE_ADD(DATE(ie.intime), interval 1 day)
  -- exclude rows marked as error
  AND c.error != 1
  group by ie.icustay_id
)
-- 5% of patients are missing a weight, but we can impute weight using their echo notes
, echo2 as(
  select ie.icustay_id, avg(weight * 0.45359237) as weight
  from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
--   left join echodata echo
--     on ie.hadm_id = echo.hadm_id
--     and echo.charttime > DATE_ADD(ie.intime, interval -7 day)
--     and echo.charttime < DATE_ADD(ie.intime, interval 1 day)
  group by ie.icustay_id
)
, vaso_cv as
(
  select ie.icustay_id
    -- case statement determining whether the ITEMID is an instance of vasopressor usage
    , max(case
            when itemid = 30047 then rate / coalesce(wt.weight,ec.weight) -- measured in mcgmin
            when itemid = 30120 then rate -- measured in mcgkgmin ** there are clear errors, perhaps actually mcgmin
            else null
          end) as rate_norepinephrine

    , max(case
            when itemid =  30044 then rate / coalesce(wt.weight,ec.weight) -- measured in mcgmin
            when itemid in (30119,30309) then rate -- measured in mcgkgmin
            else null
          end) as rate_epinephrine

    , max(case when itemid in (30043,30307) then rate end) as rate_dopamine
    , max(case when itemid in (30042,30306) then rate end) as rate_dobutamine

  from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
  inner join `bgse-dsc.MIMIC3_V1_4.INPUTEVENTS_CV` cv
    on ie.icustay_id = cv.icustay_id and cv.charttime between ie.intime and DATE_ADD(ie.intime, interval 1 day)
  left join wt
    on ie.icustay_id = wt.icustay_id
  left join echo2 ec
    on ie.icustay_id = ec.icustay_id
  where itemid in (30047,30120,30044,30119,30309,30043,30307,30042,30306)
  and rate is not null
  group by ie.icustay_id
)
, vaso_mv as
(
  select ie.icustay_id
    -- case statement determining whether the ITEMID is an instance of vasopressor usage
    , max(case when itemid = 221906 then rate end) as rate_norepinephrine
    , max(case when itemid = 221289 then rate end) as rate_epinephrine
    , max(case when itemid = 221662 then rate end) as rate_dopamine
    , max(case when itemid = 221653 then rate end) as rate_dobutamine
  from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
  inner join `bgse-dsc.MIMIC3_V1_4.INPUTEVENTS_MV` mv
    on ie.icustay_id = mv.icustay_id and mv.starttime between ie.intime and DATE_ADD(ie.intime, interval 1 day)
  where itemid in (221906,221289,221662,221653)
  -- 'Rewritten' orders are not delivered to the patient
  and statusdescription != 'Rewritten'
  group by ie.icustay_id
)
, pafi1 as
(
  -- join blood gas to ventilation durations to determine if patient was vent
  select bg.icustay_id, bg.charttime
  , PaO2FiO2
  , case when vd.icustay_id is not null then 1 else 0 end as IsVent
  from `bgse-dsc.MIMIC3_V1_4.VK_bloodgasfirstdayarterial` bg
  left join `bgse-dsc.MIMIC3_V1_4.VK_ventdurations` vd
    on bg.icustay_id = vd.icustay_id
    and bg.charttime >= vd.starttime
    and bg.charttime <= vd.endtime
  order by bg.icustay_id, bg.charttime
)
, pafi2 as
(
  -- because pafi has an interaction between vent/PaO2:FiO2, we need two columns for the score
  -- it can happen that the lowest unventilated PaO2/FiO2 is 68, but the lowest ventilated PaO2/FiO2 is 120
  -- in this case, the SOFA score is 3, *not* 4.
  select icustay_id
  , min(case when IsVent = 0 then PaO2FiO2 else null end) as PaO2FiO2_novent_min
  , min(case when IsVent = 1 then PaO2FiO2 else null end) as PaO2FiO2_vent_min
  from pafi1
  group by icustay_id
)
-- Aggregate the components for the score
, scorecomp as
(
select ie.icustay_id
  , v.MeanBP_Min
  , coalesce(cv.rate_norepinephrine, mv.rate_norepinephrine) as rate_norepinephrine
  , coalesce(cv.rate_epinephrine, mv.rate_epinephrine) as rate_epinephrine
  , coalesce(cv.rate_dopamine, mv.rate_dopamine) as rate_dopamine
  , coalesce(cv.rate_dobutamine, mv.rate_dobutamine) as rate_dobutamine

  , l.Creatinine_Max
  , l.Bilirubin_Max
  , l.Platelet_Min

  , pf.PaO2FiO2_novent_min
  , pf.PaO2FiO2_vent_min

  , uo.UrineOutput

  , gcs.MinGCS
from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
left join vaso_cv cv
  on ie.icustay_id = cv.icustay_id
left join vaso_mv mv
  on ie.icustay_id = mv.icustay_id
left join pafi2 pf
 on ie.icustay_id = pf.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_vitalsfirstday` v
  on ie.icustay_id = v.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_labsfirstday` l
  on ie.icustay_id = l.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_uofirstday` uo
  on ie.icustay_id = uo.icustay_id
left join `bgse-dsc.MIMIC3_V1_4.VK_gcsfirstday` gcs
  on ie.icustay_id = gcs.icustay_id
)
, scorecalc as
(
  -- Calculate the final score
  -- note that if the underlying data is missing, the component is null
  -- eventually these are treated as 0 (normal), but knowing when data is missing is useful for debugging
  select icustay_id
  -- Respiration
  , case
      when PaO2FiO2_vent_min   < 100 then 4
      when PaO2FiO2_vent_min   < 200 then 3
      when PaO2FiO2_novent_min < 300 then 2
      when PaO2FiO2_novent_min < 400 then 1
      when coalesce(PaO2FiO2_vent_min, PaO2FiO2_novent_min) is null then null
      else 0
    end as respiration

  -- Coagulation
  , case
      when platelet_min < 20  then 4
      when platelet_min < 50  then 3
      when platelet_min < 100 then 2
      when platelet_min < 150 then 1
      when platelet_min is null then null
      else 0
    end as coagulation

  -- Liver
  , case
      -- Bilirubin checks in mg/dL
        when Bilirubin_Max >= 12.0 then 4
        when Bilirubin_Max >= 6.0  then 3
        when Bilirubin_Max >= 2.0  then 2
        when Bilirubin_Max >= 1.2  then 1
        when Bilirubin_Max is null then null
        else 0
      end as liver

  -- Cardiovascular
  , case
      when rate_dopamine > 15 or rate_epinephrine >  0.1 or rate_norepinephrine >  0.1 then 4
      when rate_dopamine >  5 or rate_epinephrine <= 0.1 or rate_norepinephrine <= 0.1 then 3
      when rate_dopamine >  0 or rate_dobutamine > 0 then 2
      when MeanBP_Min < 70 then 1
      when coalesce(MeanBP_Min, rate_dopamine, rate_dobutamine, rate_epinephrine, rate_norepinephrine) is null then null
      else 0
    end as cardiovascular

  -- Neurological failure (GCS)
  , case
      when (MinGCS >= 13 and MinGCS <= 14) then 1
      when (MinGCS >= 10 and MinGCS <= 12) then 2
      when (MinGCS >=  6 and MinGCS <=  9) then 3
      when  MinGCS <   6 then 4
      when  MinGCS is null then null
  else 0 end
    as cns

  -- Renal failure - high creatinine or low urine output
  , case
    when (Creatinine_Max >= 5.0) then 4
    when  UrineOutput < 200 then 4
    when (Creatinine_Max >= 3.5 and Creatinine_Max < 5.0) then 3
    when  UrineOutput < 500 then 3
    when (Creatinine_Max >= 2.0 and Creatinine_Max < 3.5) then 2
    when (Creatinine_Max >= 1.2 and Creatinine_Max < 2.0) then 1
    when coalesce(UrineOutput, Creatinine_Max) is null then null
  else 0 end
    as renal
  from scorecomp
)
select ie.subject_id, ie.hadm_id, ie.icustay_id
  -- Combine all the scores to get SOFA
  -- Impute 0 if the score is missing
  , coalesce(respiration,0)
  + coalesce(coagulation,0)
  + coalesce(liver,0)
  + coalesce(cardiovascular,0)
  + coalesce(cns,0)
  + coalesce(renal,0)
  as SOFA
, respiration
, coagulation
, liver
, cardiovascular
, cns
, renal
from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
left join scorecalc s
  on ie.icustay_id = s.icustay_id
order by ie.icustay_id;




- echodata

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_ECHODATA` AS
select ROW_ID
  , subject_id, hadm_id
  , chartdate

  -- charttime is always null for echoes..
  -- however, the time is available in the echo text, e.g.:
  -- , substring(ne.text, 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)') as TIMESTAMP
  -- we can therefore impute it and re-create charttime
  , cast(to_timestamp( (to_char( chartdate, 'DD-MM-YYYY' ) || substring(ne.text, 'Date/Time: [\[\]0-9*-]+ at ([0-9:]+)')),
            'DD-MM-YYYYHH24:MI') as timestamp without time zone)
    as charttime

  -- explanation of below substring:
  --  'Indication: ' - matched verbatim
  --  (.*?) - match any character
  --  \n - the end of the line
  -- substring only returns the item in ()s
  -- note: the '?' makes it non-greedy. if you exclude it, it matches until it reaches the *last* \n

  , substring(ne.text, 'Indication: (.*?)\n') as Indication

  -- sometimes numeric values contain de-id text, e.g. [** Numeric Identifier **]
  -- this removes that text
  , case
      when substring(ne.text, 'Height: \(in\) (.*?)\n') like '%*%'
        then null
      else cast(substring(ne.text, 'Height: \(in\) (.*?)\n') as numeric)
    end as Height

  , case
      when substring(ne.text, 'Weight \(lb\): (.*?)\n') like '%*%'
        then null
      else cast(substring(ne.text, 'Weight \(lb\): (.*?)\n') as numeric)
    end as Weight

  , case
      when substring(ne.text, 'BSA \(m2\): (.*?) m2\n') like '%*%'
        then null
      else cast(substring(ne.text, 'BSA \(m2\): (.*?) m2\n') as numeric)
    end as BSA -- ends in 'm2'

  , substring(ne.text, 'BP \(mm Hg\): (.*?)\n') as BP -- Sys/Dias

  , case
      when substring(ne.text, 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') like '%*%'
        then null
      else cast(substring(ne.text, 'BP \(mm Hg\): ([0-9]+)/[0-9]+?\n') as numeric)
    end as BPSys -- first part of fraction

  , case
      when substring(ne.text, 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') like '%*%'
        then null
      else cast(substring(ne.text, 'BP \(mm Hg\): [0-9]+/([0-9]+?)\n') as numeric)
    end as BPDias -- second part of fraction

  , case
      when substring(ne.text, 'HR \(bpm\): ([0-9]+?)\n') like '%*%'
        then null
      else cast(substring(ne.text, 'HR \(bpm\): ([0-9]+?)\n') as numeric)
    end as HR

  , substring(ne.text, 'Status: (.*?)\n') as Status
  , substring(ne.text, 'Test: (.*?)\n') as Test
  , substring(ne.text, 'Doppler: (.*?)\n') as Doppler
  , substring(ne.text, 'Contrast: (.*?)\n') as Contrast
  , substring(ne.text, 'Technical Quality: (.*?)\n') as TechnicalQuality
from noteevents ne
where category = 'Echo';



-- labsfirstday (CREATED)

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_labsfirstday` AS
SELECT
  pvt.subject_id, pvt.hadm_id, pvt.icustay_id

  , min(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END) as ANIONGAP_min
  , max(CASE WHEN label = 'ANION GAP' THEN valuenum ELSE null END) as ANIONGAP_max
  , min(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_min
  , max(CASE WHEN label = 'ALBUMIN' THEN valuenum ELSE null END) as ALBUMIN_max
  , min(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END) as BANDS_min
  , max(CASE WHEN label = 'BANDS' THEN valuenum ELSE null END) as BANDS_max
  , min(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_min
  , max(CASE WHEN label = 'BICARBONATE' THEN valuenum ELSE null END) as BICARBONATE_max
  , min(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END) as BILIRUBIN_min
  , max(CASE WHEN label = 'BILIRUBIN' THEN valuenum ELSE null END) as BILIRUBIN_max
  , min(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END) as CREATININE_min
  , max(CASE WHEN label = 'CREATININE' THEN valuenum ELSE null END) as CREATININE_max
  , min(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END) as CHLORIDE_min
  , max(CASE WHEN label = 'CHLORIDE' THEN valuenum ELSE null END) as CHLORIDE_max
  , min(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END) as GLUCOSE_min
  , max(CASE WHEN label = 'GLUCOSE' THEN valuenum ELSE null END) as GLUCOSE_max
  , min(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END) as HEMATOCRIT_min
  , max(CASE WHEN label = 'HEMATOCRIT' THEN valuenum ELSE null END) as HEMATOCRIT_max
  , min(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_min
  , max(CASE WHEN label = 'HEMOGLOBIN' THEN valuenum ELSE null END) as HEMOGLOBIN_max
  , min(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END) as LACTATE_min
  , max(CASE WHEN label = 'LACTATE' THEN valuenum ELSE null END) as LACTATE_max
  , min(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END) as PLATELET_min
  , max(CASE WHEN label = 'PLATELET' THEN valuenum ELSE null END) as PLATELET_max
  , min(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END) as POTASSIUM_min
  , max(CASE WHEN label = 'POTASSIUM' THEN valuenum ELSE null END) as POTASSIUM_max
  , min(CASE WHEN label = 'PTT' THEN valuenum ELSE null END) as PTT_min
  , max(CASE WHEN label = 'PTT' THEN valuenum ELSE null END) as PTT_max
  , min(CASE WHEN label = 'INR' THEN valuenum ELSE null END) as INR_min
  , max(CASE WHEN label = 'INR' THEN valuenum ELSE null END) as INR_max
  , min(CASE WHEN label = 'PT' THEN valuenum ELSE null END) as PT_min
  , max(CASE WHEN label = 'PT' THEN valuenum ELSE null END) as PT_max
  , min(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null END) as SODIUM_min
  , max(CASE WHEN label = 'SODIUM' THEN valuenum ELSE null end) as SODIUM_max
  , min(CASE WHEN label = 'BUN' THEN valuenum ELSE null end) as BUN_min
  , max(CASE WHEN label = 'BUN' THEN valuenum ELSE null end) as BUN_max
  , min(CASE WHEN label = 'WBC' THEN valuenum ELSE null end) as WBC_min
  , max(CASE WHEN label = 'WBC' THEN valuenum ELSE null end) as WBC_max


FROM
( -- begin query that extracts the data
  SELECT ie.subject_id, ie.hadm_id, ie.icustay_id
  -- here we assign labels to ITEMIDs
  -- this also fuses together multiple ITEMIDs containing the same data
  , CASE
        WHEN itemid = 50868 THEN 'ANION GAP'
        WHEN itemid = 50862 THEN 'ALBUMIN'
        WHEN itemid = 51144 THEN 'BANDS'
        WHEN itemid = 50882 THEN 'BICARBONATE'
        WHEN itemid = 50885 THEN 'BILIRUBIN'
        WHEN itemid = 50912 THEN 'CREATININE'
        WHEN itemid = 50806 THEN 'CHLORIDE'
        WHEN itemid = 50902 THEN 'CHLORIDE'
        WHEN itemid = 50809 THEN 'GLUCOSE'
        WHEN itemid = 50931 THEN 'GLUCOSE'
        WHEN itemid = 50810 THEN 'HEMATOCRIT'
        WHEN itemid = 51221 THEN 'HEMATOCRIT'
        WHEN itemid = 50811 THEN 'HEMOGLOBIN'
        WHEN itemid = 51222 THEN 'HEMOGLOBIN'
        WHEN itemid = 50813 THEN 'LACTATE'
        WHEN itemid = 51265 THEN 'PLATELET'
        WHEN itemid = 50822 THEN 'POTASSIUM'
        WHEN itemid = 50971 THEN 'POTASSIUM'
        WHEN itemid = 51275 THEN 'PTT'
        WHEN itemid = 51237 THEN 'INR'
        WHEN itemid = 51274 THEN 'PT'
        WHEN itemid = 50824 THEN 'SODIUM'
        WHEN itemid = 50983 THEN 'SODIUM'
        WHEN itemid = 51006 THEN 'BUN'
        WHEN itemid = 51300 THEN 'WBC'
        WHEN itemid = 51301 THEN 'WBC'
      ELSE null
    END AS label
  , -- add in some sanity checks on the values
  -- the where clause below requires all valuenum to be > 0, so these are only upper limit checks
    CASE
      WHEN itemid = 50862 and valuenum >    10 THEN null -- g/dL 'ALBUMIN'
      WHEN itemid = 50868 and valuenum > 10000 THEN null -- mEq/L 'ANION GAP'
      WHEN itemid = 51144 and valuenum <     0 THEN null -- immature band forms, %
      WHEN itemid = 51144 and valuenum >   100 THEN null -- immature band forms, %
      WHEN itemid = 50882 and valuenum > 10000 THEN null -- mEq/L 'BICARBONATE'
      WHEN itemid = 50885 and valuenum >   150 THEN null -- mg/dL 'BILIRUBIN'
      WHEN itemid = 50806 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
      WHEN itemid = 50902 and valuenum > 10000 THEN null -- mEq/L 'CHLORIDE'
      WHEN itemid = 50912 and valuenum >   150 THEN null -- mg/dL 'CREATININE'
      WHEN itemid = 50809 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
      WHEN itemid = 50931 and valuenum > 10000 THEN null -- mg/dL 'GLUCOSE'
      WHEN itemid = 50810 and valuenum >   100 THEN null -- % 'HEMATOCRIT'
      WHEN itemid = 51221 and valuenum >   100 THEN null -- % 'HEMATOCRIT'
      WHEN itemid = 50811 and valuenum >    50 THEN null -- g/dL 'HEMOGLOBIN'
      WHEN itemid = 51222 and valuenum >    50 THEN null -- g/dL 'HEMOGLOBIN'
      WHEN itemid = 50813 and valuenum >    50 THEN null -- mmol/L 'LACTATE'
      WHEN itemid = 51265 and valuenum > 10000 THEN null -- K/uL 'PLATELET'
      WHEN itemid = 50822 and valuenum >    30 THEN null -- mEq/L 'POTASSIUM'
      WHEN itemid = 50971 and valuenum >    30 THEN null -- mEq/L 'POTASSIUM'
      WHEN itemid = 51275 and valuenum >   150 THEN null -- sec 'PTT'
      WHEN itemid = 51237 and valuenum >    50 THEN null -- 'INR'
      WHEN itemid = 51274 and valuenum >   150 THEN null -- sec 'PT'
      WHEN itemid = 50824 and valuenum >   200 THEN null -- mEq/L == mmol/L 'SODIUM'
      WHEN itemid = 50983 and valuenum >   200 THEN null -- mEq/L == mmol/L 'SODIUM'
      WHEN itemid = 51006 and valuenum >   300 THEN null -- 'BUN'
      WHEN itemid = 51300 and valuenum >  1000 THEN null -- 'WBC'
      WHEN itemid = 51301 and valuenum >  1000 THEN null -- 'WBC'
    ELSE le.valuenum
    END AS valuenum

  FROM `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie

  LEFT JOIN `bgse-dsc.MIMIC3_V1_4.LABEVENTS` le
    ON le.subject_id = ie.subject_id AND le.hadm_id = ie.hadm_id
    AND TIMESTAMP(le.charttime) BETWEEN (TIMESTAMP_ADD(TIMESTAMP(ie.intime), interval -6 hour)) AND (TIMESTAMP_ADD(TIMESTAMP(ie.intime), interval 1 day))
    AND le.ITEMID in
    (
      -- comment is: LABEL | CATEGORY | FLUID | NUMBER OF ROWS IN LABEVENTS
      50868, -- ANION GAP | CHEMISTRY | BLOOD | 769895
      50862, -- ALBUMIN | CHEMISTRY | BLOOD | 146697
      51144, -- BANDS - hematology
      50882, -- BICARBONATE | CHEMISTRY | BLOOD | 780733
      50885, -- BILIRUBIN, TOTAL | CHEMISTRY | BLOOD | 238277
      50912, -- CREATININE | CHEMISTRY | BLOOD | 797476
      50902, -- CHLORIDE | CHEMISTRY | BLOOD | 795568
      50806, -- CHLORIDE, WHOLE BLOOD | BLOOD GAS | BLOOD | 48187
      50931, -- GLUCOSE | CHEMISTRY | BLOOD | 748981
      50809, -- GLUCOSE | BLOOD GAS | BLOOD | 196734
      51221, -- HEMATOCRIT | HEMATOLOGY | BLOOD | 881846
      50810, -- HEMATOCRIT, CALCULATED | BLOOD GAS | BLOOD | 89715
      51222, -- HEMOGLOBIN | HEMATOLOGY | BLOOD | 752523
      50811, -- HEMOGLOBIN | BLOOD GAS | BLOOD | 89712
      50813, -- LACTATE | BLOOD GAS | BLOOD | 187124
      51265, -- PLATELET COUNT | HEMATOLOGY | BLOOD | 778444
      50971, -- POTASSIUM | CHEMISTRY | BLOOD | 845825
      50822, -- POTASSIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 192946
      51275, -- PTT | HEMATOLOGY | BLOOD | 474937
      51237, -- INR(PT) | HEMATOLOGY | BLOOD | 471183
      51274, -- PT | HEMATOLOGY | BLOOD | 469090
      50983, -- SODIUM | CHEMISTRY | BLOOD | 808489
      50824, -- SODIUM, WHOLE BLOOD | BLOOD GAS | BLOOD | 71503
      51006, -- UREA NITROGEN | CHEMISTRY | BLOOD | 791925
      51301, -- WHITE BLOOD CELLS | HEMATOLOGY | BLOOD | 753301
      51300  -- WBC COUNT | HEMATOLOGY | BLOOD | 2371
    )
    AND valuenum IS NOT null AND valuenum > 0 -- lab values cannot be 0 and cannot be negative
) pvt
GROUP BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id
ORDER BY pvt.subject_id, pvt.hadm_id, pvt.icustay_id;



-- blood gas first day arterial (CREATED)

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_bloodgasfirstdayarterial` AS
with stg_spo2 as
(
  select SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
    -- max here is just used to group SpO2 by charttime
    , max(case when valuenum <= 0 or valuenum > 100 then null else valuenum end) as SpO2
  from `bgse-dsc.MIMIC3_V1_4.CHARTEVENTS`
  -- o2 sat
  where ITEMID in
  (
    646 -- SpO2
  , 220277 -- O2 saturation pulseoxymetry
  )
  group by SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
)
, stg_fio2 as
(
  select SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
    -- pre-process the FiO2s to ensure they are between 21-100%
    , max(
        case
          when itemid = 223835
            then case
              when valuenum > 0 and valuenum <= 1
                then valuenum * 100
              -- improperly input data - looks like O2 flow in litres
              when valuenum > 1 and valuenum < 21
                then null
              when valuenum >= 21 and valuenum <= 100
                then valuenum
              else null end -- unphysiological
        when itemid in (3420, 3422)
        -- all these values are well formatted
            then valuenum
        when itemid = 190 and valuenum > 0.20 and valuenum < 1
        -- well formatted but not in %
            then valuenum * 100
      else null end
    ) as fio2_chartevents
  from `bgse-dsc.MIMIC3_V1_4.CHARTEVENTS`
  where ITEMID in
  (
    3420 -- FiO2
  , 190 -- FiO2 set
  , 223835 -- Inspired O2 Fraction (FiO2)
  , 3422 -- FiO2 [measured]
  )
  -- exclude rows marked as error
  and error != 1
  group by SUBJECT_ID, HADM_ID, ICUSTAY_ID, CHARTTIME
)
, stg2 as
(
select bg.*
  , ROW_NUMBER() OVER (partition by bg.icustay_id, bg.charttime order by s1.charttime DESC) as lastRowSpO2
  , s1.spo2
from `bgse-dsc.MIMIC3_V1_4.VK_bloodgasfirstday` bg
left join stg_spo2 s1
  -- same patient
  on  bg.icustay_id = s1.icustay_id
  -- spo2 occurred at most 2 hours before this blood gas
  and TIMESTAMP(s1.charttime) between (TIMESTAMP_ADD(TIMESTAMP(bg.charttime), interval -2 hour)) and TIMESTAMP(bg.charttime)
where bg.po2 is not null
)
, stg3 as
(
select bg.*
  , ROW_NUMBER() OVER (partition by bg.icustay_id, bg.charttime order by s2.charttime DESC) as lastRowFiO2
  , s2.fio2_chartevents

  -- create our specimen prediction
  ,  1/(1+exp(-(-0.02544
  +    0.04598 * po2
  + coalesce(-0.15356 * spo2             , -0.15356 *   97.49420 +    0.13429)
  + coalesce( 0.00621 * fio2_chartevents ,  0.00621 *   51.49550 +   -0.24958)
  + coalesce( 0.10559 * hemoglobin       ,  0.10559 *   10.32307 +    0.05954)
  + coalesce( 0.13251 * so2              ,  0.13251 *   93.66539 +   -0.23172)
  + coalesce(-0.01511 * pco2             , -0.01511 *   42.08866 +   -0.01630)
  + coalesce( 0.01480 * fio2             ,  0.01480 *   63.97836 +   -0.31142)
  + coalesce(-0.00200 * aado2            , -0.00200 *  442.21186 +   -0.01328)
  + coalesce(-0.03220 * bicarbonate      , -0.03220 *   22.96894 +   -0.06535)
  + coalesce( 0.05384 * totalco2         ,  0.05384 *   24.72632 +   -0.01405)
  + coalesce( 0.08202 * lactate          ,  0.08202 *    3.06436 +    0.06038)
  + coalesce( 0.10956 * ph               ,  0.10956 *    7.36233 +   -0.00617)
  + coalesce( 0.00848 * o2flow           ,  0.00848 *    7.59362 +   -0.35803)
  ))) as SPECIMEN_PROB
from stg2 bg
left join stg_fio2 s2
  -- same patient
  on  bg.icustay_id = s2.icustay_id
  -- fio2 occurred at most 4 hours before this blood gas
  and TIMESTAMP(s2.charttime) between (TIMESTAMP_ADD(TIMESTAMP(bg.charttime), interval -4 hour)) and TIMESTAMP(bg.charttime)
where bg.lastRowSpO2 = 1 -- only the row with the most recent SpO2 (if no SpO2 found lastRowSpO2 = 1)
)

select subject_id, hadm_id,
icustay_id, charttime
, SPECIMEN -- raw data indicating sample type, only present 80% of the time

-- prediction of specimen for missing data
, case
      when SPECIMEN is not null then SPECIMEN
      when SPECIMEN_PROB > 0.75 then 'ART'
    else null end as SPECIMEN_PRED
, SPECIMEN_PROB

-- oxygen related parameters
, SO2, spo2 -- note spo2 is from chartevents
, PO2, PCO2
, fio2_chartevents, FIO2
, AADO2
-- also calculate AADO2
, case
    when  PO2 is not null
      and pco2 is not null
      and coalesce(FIO2, fio2_chartevents) is not null
     -- multiple by 100 because FiO2 is in a % but should be a fraction
      then (coalesce(FIO2, fio2_chartevents)/100) * (760 - 47) - (pco2/0.8) - po2
    else null
  end as AADO2_calc
, case
    when PO2 is not null and coalesce(FIO2, fio2_chartevents) is not null
     -- multiply by 100 because FiO2 is in a % but should be a fraction
      then 100*PO2/(coalesce(FIO2, fio2_chartevents))
    else null
  end as PaO2FiO2
-- acid-base parameters
, PH, BASEEXCESS
, BICARBONATE, TOTALCO2

-- blood count parameters
, HEMATOCRIT
, HEMOGLOBIN
, CARBOXYHEMOGLOBIN
, METHEMOGLOBIN

-- chemistry
, CHLORIDE, CALCIUM
, TEMPERATURE
, POTASSIUM, SODIUM
, LACTATE
, GLUCOSE

-- ventilation stuff that's sometimes input
, INTUBATED, TIDALVOLUME, VENTILATIONRATE, VENTILATOR
, PEEP, O2Flow
, REQUIREDO2

from stg3
where lastRowFiO2 = 1 -- only the most recent FiO2
-- restrict it to *only* arterial samples
and (SPECIMEN = 'ART' or SPECIMEN_PROB > 0.75)
order by icustay_id, charttime;



-- blood gas first day (CREATED)

create TABLE `bgse-dsc.MIMIC3_V1_4.VK_bloodgasfirstday` as
with pvt as
( -- begin query that extracts the data
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  -- here we assign labels to ITEMIDs
  -- this also fuses together multiple ITEMIDs containing the same data
      , case
        when itemid = 50800 then 'SPECIMEN'
        when itemid = 50801 then 'AADO2'
        when itemid = 50802 then 'BASEEXCESS'
        when itemid = 50803 then 'BICARBONATE'
        when itemid = 50804 then 'TOTALCO2'
        when itemid = 50805 then 'CARBOXYHEMOGLOBIN'
        when itemid = 50806 then 'CHLORIDE'
        when itemid = 50808 then 'CALCIUM'
        when itemid = 50809 then 'GLUCOSE'
        when itemid = 50810 then 'HEMATOCRIT'
        when itemid = 50811 then 'HEMOGLOBIN'
        when itemid = 50812 then 'INTUBATED'
        when itemid = 50813 then 'LACTATE'
        when itemid = 50814 then 'METHEMOGLOBIN'
        when itemid = 50815 then 'O2FLOW'
        when itemid = 50816 then 'FIO2'
        when itemid = 50817 then 'SO2' -- OXYGENSATURATION
        when itemid = 50818 then 'PCO2'
        when itemid = 50819 then 'PEEP'
        when itemid = 50820 then 'PH'
        when itemid = 50821 then 'PO2'
        when itemid = 50822 then 'POTASSIUM'
        when itemid = 50823 then 'REQUIREDO2'
        when itemid = 50824 then 'SODIUM'
        when itemid = 50825 then 'TEMPERATURE'
        when itemid = 50826 then 'TIDALVOLUME'
        when itemid = 50827 then 'VENTILATIONRATE'
        when itemid = 50828 then 'VENTILATOR'
        else null
        end as label
        , charttime
        , value
        -- add in some sanity checks on the values
        , case
          when valuenum <= 0 and itemid != 50802 then null -- allow negative baseexcess
          when itemid = 50810 and valuenum > 100 then null -- hematocrit
          -- ensure FiO2 is a valid number between 21-100
          -- mistakes are rare (<100 obs out of ~100,000)
          -- there are 862 obs of valuenum == 20 - some people round down!
          -- rather than risk imputing garbage data for FiO2, we simply NULL invalid values
          when itemid = 50816 and valuenum < 20 then null
          when itemid = 50816 and valuenum > 100 then null
          when itemid = 50817 and valuenum > 100 then null -- O2 sat
          when itemid = 50815 and valuenum >  70 then null -- O2 flow
          when itemid = 50821 and valuenum > 800 then null -- PO2
           -- conservative upper limit
        else valuenum
        end as valuenum

    from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS` ie
    left join `bgse-dsc.MIMIC3_V1_4.LABEVENTS` le
      on le.subject_id = ie.subject_id and le.hadm_id = ie.hadm_id
      and TIMESTAMP(le.charttime) between (TIMESTAMP_ADD(TIMESTAMP(ie.intime), interval -6 hour)) and (TIMESTAMP_ADD(TIMESTAMP(ie.intime), interval 1 day))
      and le.ITEMID in
      -- blood gases
      (
        50800, 50801, 50802, 50803, 50804, 50805, 50806, 50807, 50808, 50809
        , 50810, 50811, 50812, 50813, 50814, 50815, 50816, 50817, 50818, 50819
        , 50820, 50821, 50822, 50823, 50824, 50825, 50826, 50827, 50828
        , 51545
      )
)
select pvt.SUBJECT_ID, pvt.HADM_ID, pvt.ICUSTAY_ID, pvt.CHARTTIME
, max(case when label = 'SPECIMEN' then value else null end) as SPECIMEN
, max(case when label = 'AADO2' then valuenum else null end) as AADO2
, max(case when label = 'BASEEXCESS' then valuenum else null end) as BASEEXCESS
, max(case when label = 'BICARBONATE' then valuenum else null end) as BICARBONATE
, max(case when label = 'TOTALCO2' then valuenum else null end) as TOTALCO2
, max(case when label = 'CARBOXYHEMOGLOBIN' then valuenum else null end) as CARBOXYHEMOGLOBIN
, max(case when label = 'CHLORIDE' then valuenum else null end) as CHLORIDE
, max(case when label = 'CALCIUM' then valuenum else null end) as CALCIUM
, max(case when label = 'GLUCOSE' then valuenum else null end) as GLUCOSE
, max(case when label = 'HEMATOCRIT' then valuenum else null end) as HEMATOCRIT
, max(case when label = 'HEMOGLOBIN' then valuenum else null end) as HEMOGLOBIN
, max(case when label = 'INTUBATED' then valuenum else null end) as INTUBATED
, max(case when label = 'LACTATE' then valuenum else null end) as LACTATE
, max(case when label = 'METHEMOGLOBIN' then valuenum else null end) as METHEMOGLOBIN
, max(case when label = 'O2FLOW' then valuenum else null end) as O2FLOW
, max(case when label = 'FIO2' then valuenum else null end) as FIO2
, max(case when label = 'SO2' then valuenum else null end) as SO2 -- OXYGENSATURATION
, max(case when label = 'PCO2' then valuenum else null end) as PCO2
, max(case when label = 'PEEP' then valuenum else null end) as PEEP
, max(case when label = 'PH' then valuenum else null end) as PH
, max(case when label = 'PO2' then valuenum else null end) as PO2
, max(case when label = 'POTASSIUM' then valuenum else null end) as POTASSIUM
, max(case when label = 'REQUIREDO2' then valuenum else null end) as REQUIREDO2
, max(case when label = 'SODIUM' then valuenum else null end) as SODIUM
, max(case when label = 'TEMPERATURE' then valuenum else null end) as TEMPERATURE
, max(case when label = 'TIDALVOLUME' then valuenum else null end) as TIDALVOLUME
, max(case when label = 'VENTILATIONRATE' then valuenum else null end) as VENTILATIONRATE
, max(case when label = 'VENTILATOR' then valuenum else null end) as VENTILATOR
from pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id, pvt.CHARTTIME;

-- allscores

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_allscores` AS
(SELECT *
FROM `bgse-dsc.MIMIC3_V1_4.VK_oasis_v2`
FULL OUTER JOIN (SELECT * EXCEPT (subject_id, hadm_id, age_score, temp_score, gcs_score, hr_score, sysbp_score, uo_score, bun_score, wbc_score, potassium_score, sodium_score, bicarbonate_score) FROM `bgse-dsc.MIMIC3_V1_4.VK_SAPS_v2`) USING (icustay_id)
FULL OUTER JOIN (SELECT * EXCEPT (subject_id, hadm_id, age_score, temp_score, gcs_score, hr_score, sysbp_score, uo_score, bun_score, wbc_score, potassium_score, sodium_score, bicarbonate_score) FROM `bgse-dsc.MIMIC3_V1_4.VK_SAPSII_v2`) USING (icustay_id)
FULL OUTER JOIN (SELECT * EXCEPT (subject_id, hadm_id) FROM `bgse-dsc.MIMIC3_V1_4.VK_SOFA_v2`) USING (icustay_id))


## FIRSTDAY VITALS


CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_vitalsfirstday` AS
SELECT pvt.subject_id, pvt.hadm_id, pvt.icustay_id

-- Easier names
, min(case when VitalID = 1 then valuenum else null end) as HeartRate_Min
, max(case when VitalID = 1 then valuenum else null end) as HeartRate_Max
, avg(case when VitalID = 1 then valuenum else null end) as HeartRate_Mean
, min(case when VitalID = 2 then valuenum else null end) as SysBP_Min
, max(case when VitalID = 2 then valuenum else null end) as SysBP_Max
, avg(case when VitalID = 2 then valuenum else null end) as SysBP_Mean
, min(case when VitalID = 3 then valuenum else null end) as DiasBP_Min
, max(case when VitalID = 3 then valuenum else null end) as DiasBP_Max
, avg(case when VitalID = 3 then valuenum else null end) as DiasBP_Mean
, min(case when VitalID = 4 then valuenum else null end) as MeanBP_Min
, max(case when VitalID = 4 then valuenum else null end) as MeanBP_Max
, avg(case when VitalID = 4 then valuenum else null end) as MeanBP_Mean
, min(case when VitalID = 5 then valuenum else null end) as RespRate_Min
, max(case when VitalID = 5 then valuenum else null end) as RespRate_Max
, avg(case when VitalID = 5 then valuenum else null end) as RespRate_Mean
, min(case when VitalID = 6 then valuenum else null end) as TempC_Min
, max(case when VitalID = 6 then valuenum else null end) as TempC_Max
, avg(case when VitalID = 6 then valuenum else null end) as TempC_Mean
, min(case when VitalID = 7 then valuenum else null end) as SpO2_Min
, max(case when VitalID = 7 then valuenum else null end) as SpO2_Max
, avg(case when VitalID = 7 then valuenum else null end) as SpO2_Mean
, min(case when VitalID = 8 then valuenum else null end) as Glucose_Min
, max(case when VitalID = 8 then valuenum else null end) as Glucose_Max
, avg(case when VitalID = 8 then valuenum else null end) as Glucose_Mean

FROM  (
  select ie.subject_id, ie.hadm_id, ie.icustay_id
  , case
    when itemid in (211,220045) and valuenum > 0 and valuenum < 300 then 1 -- HeartRate
    when itemid in (51,442,455,6701,220179,220050) and valuenum > 0 and valuenum < 400 then 2 -- SysBP
    when itemid in (8368,8440,8441,8555,220180,220051) and valuenum > 0 and valuenum < 300 then 3 -- DiasBP
    when itemid in (456,52,6702,443,220052,220181,225312) and valuenum > 0 and valuenum < 300 then 4 -- MeanBP
    when itemid in (615,618,220210,224690) and valuenum > 0 and valuenum < 70 then 5 -- RespRate
    when itemid in (223761,678) and valuenum > 70 and valuenum < 120  then 6 -- TempF, converted to degC in valuenum call
    when itemid in (223762,676) and valuenum > 10 and valuenum < 50  then 6 -- TempC
    when itemid in (646,220277) and valuenum > 0 and valuenum <= 100 then 7 -- SpO2
    when itemid in (807,811,1529,3745,3744,225664,220621,226537) and valuenum > 0 then 8 -- Glucose

    else null end as VitalID
      -- convert F to C
  , case when itemid in (223761,678) then (valuenum-32)/1.8 else valuenum end as valuenum

  from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS_TRANS_COLLAPSED_V2` ie
  left join `bgse-dsc.MIMIC3_V1_4.CHARTEVENTS` ce
  on ie.subject_id = ce.subject_id and ie.hadm_id = ce.hadm_id and ie.icustay_id = ce.icustay_id
  -- exclude rows marked as error
  and ce.error != 1
  where ce.itemid in
  (
  -- HEART RATE
  211, --"Heart Rate"
  220045, --"Heart Rate"

  -- Systolic/diastolic

  51, --	Arterial BP [Systolic]
  442, --	Manual BP [Systolic]
  455, --	NBP [Systolic]
  6701, --	Arterial BP #2 [Systolic]
  220179, --	Non Invasive Blood Pressure systolic
  220050, --	Arterial Blood Pressure systolic

  8368, --	Arterial BP [Diastolic]
  8440, --	Manual BP [Diastolic]
  8441, --	NBP [Diastolic]
  8555, --	Arterial BP #2 [Diastolic]
  220180, --	Non Invasive Blood Pressure diastolic
  220051, --	Arterial Blood Pressure diastolic


  -- MEAN ARTERIAL PRESSURE
  456, --"NBP Mean"
  52, --"Arterial BP Mean"
  6702, --	Arterial BP Mean #2
  443, --	Manual BP Mean(calc)
  220052, --"Arterial Blood Pressure mean"
  220181, --"Non Invasive Blood Pressure mean"
  225312, --"ART BP mean"

  -- RESPIRATORY RATE
  618,--	Respiratory Rate
  615,--	Resp Rate (Total)
  220210,--	Respiratory Rate
  224690, --	Respiratory Rate (Total)


  -- SPO2, peripheral
  646, 220277,

  -- GLUCOSE, both lab and fingerstick
  807,--	Fingerstick Glucose
  811,--	Glucose (70-105)
  1529,--	Glucose
  3745,--	BloodGlucose
  3744,--	Blood Glucose
  225664,--	Glucose finger stick
  220621,--	Glucose (serum)
  226537,--	Glucose (whole blood)

  -- TEMPERATURE
  223762, -- "Temperature Celsius"
  676,	-- "Temperature C"
  223761, -- "Temperature Fahrenheit"
  678 --	"Temperature F"

  )
) pvt
group by pvt.subject_id, pvt.hadm_id, pvt.icustay_id
order by pvt.subject_id, pvt.hadm_id, pvt.icustay_id;

####### FIRSTDAY GLUCOSE

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_gcsfirstday` AS
SELECT pvt.ICUSTAY_ID
  , pvt.charttime

  -- Easier names - note we coalesced Metavision and CareVue IDs below
  , max(case when pvt.itemid = 454 then pvt.valuenum else null end) as GCSMotor
  , max(case when pvt.itemid = 723 then pvt.valuenum else null end) as GCSVerbal
  , max(case when pvt.itemid = 184 then pvt.valuenum else null end) as GCSEyes

  -- If verbal was set to 0 in the below select, then this is an intubated patient
  , case
      when max(case when pvt.itemid = 723 then pvt.valuenum else null end) = 0
    then 1
    else 0
    end as EndoTrachFlag

  , ROW_NUMBER ()
          OVER (PARTITION BY pvt.ICUSTAY_ID ORDER BY pvt.charttime ASC) as rn

  FROM  (
  select l.ICUSTAY_ID
  -- merge the ITEMIDs so that the pivot applies to both metavision/carevue data
  , case
      when l.ITEMID in (723,223900) then 723
      when l.ITEMID in (454,223901) then 454
      when l.ITEMID in (184,220739) then 184
      else l.ITEMID end
    as ITEMID

  -- convert the data into a number, reserving a value of 0 for ET/Trach
  , case
      -- endotrach/vent is assigned a value of 0, later parsed specially
      when l.ITEMID = 723 and l.VALUE = '1.0 ET/Trach' then 0 -- carevue
      when l.ITEMID = 223900 and l.VALUE = 'No Response-ETT' then 0 -- metavision

      else VALUENUM
      end
    as VALUENUM
  , l.CHARTTIME
  from `bgse-dsc.MIMIC3_V1_4.CHARTEVENTS` l

  -- get intime for charttime subselection
  inner join `bgse-dsc.MIMIC3_V1_4.ICUSTAYS_TRANS_COLLAPSED_V2` b
    on l.icustay_id = b.icustay_id

  -- Isolate the desired GCS variables
  where l.ITEMID in
  (
    -- 198 -- GCS
    -- GCS components, CareVue
    184, 454, 723
    -- GCS components, Metavision
    , 223900, 223901, 220739
  )
  -- Only get data for the first 24 hours
  -- DATE(ce.charttime) between DATE(ie.intime) and DATE_ADD(ie.intime), interval 1 DAY)
  and DATE(l.charttime) between DATE(b.intime) and DATE_ADD(DATE(b.intime), interval 1 DAY)
  -- exclude rows marked as error
  and l.error IS DISTINCT FROM 1
  ) pvt
  group by pvt.ICUSTAY_ID, pvt.charttime
)
, gcs as (
  select b.*
  , b2.GCSVerbal as GCSVerbalPrev
  , b2.GCSMotor as GCSMotorPrev
  , b2.GCSEyes as GCSEyesPrev
  -- Calculate GCS, factoring in special case when they are intubated and prev vals
  -- note that the coalesce are used to implement the following if:
  --  if current value exists, use it
  --  if previous value exists, use it
  --  otherwise, default to normal
  , case
      -- replace GCS during sedation with 15
      when b.GCSVerbal = 0
        then 15
      when b.GCSVerbal is null and b2.GCSVerbal = 0
        then 15
      -- if previously they were intub, but they aren't now, do not use previous GCS values
      when b2.GCSVerbal = 0
        then
            coalesce(b.GCSMotor,6)
          + coalesce(b.GCSVerbal,5)
          + coalesce(b.GCSEyes,4)
      -- otherwise, add up score normally, imputing previous value if none available at current time
      else
            coalesce(b.GCSMotor,coalesce(b2.GCSMotor,6))
          + coalesce(b.GCSVerbal,coalesce(b2.GCSVerbal,5))
          + coalesce(b.GCSEyes,coalesce(b2.GCSEyes,4))
      end as GCS

##### FIRSTDAY URINE OUTPUT

-- ------------------------------------------------------------------
-- Purpose: Create a view of the urine output for each ICUSTAY_ID over the first 24 hours.
-- ------------------------------------------------------------------

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_uofirstday` AS
Select
  -- patient identifiers
  ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID

  -- volumes associated with urine output ITEMIDs
  , sum(
      -- we consider input of GU irrigant as a negative volume
      case
        when oe.itemid = 227488 and oe.value > 0 then -1*oe.value
        else oe.value
    end) as UrineOutput
from `bgse-dsc.MIMIC3_V1_4.ICUSTAYS_TRANS_COLLAPSED_V2` ie
-- Join to the outputevents table to get urine output
left join `bgse-dsc.MIMIC3_V1_4.OUTPUTEVENTS` oe
-- join on all patient identifiers
on ie.subject_id = oe.subject_id and ie.hadm_id = oe.hadm_id and ie.icustay_id = oe.icustay_id
-- and ensure the data occurs during the first day
and DATE(oe.charttime) between DATE(ie.intime) and DATE_ADD(DATE(ie.intime), interval 1 DAY) -- first ICU day
where itemid in
(
-- these are the most frequently occurring urine output observations in CareVue
40055, -- "Urine Out Foley"
43175, -- "Urine ."
40069, -- "Urine Out Void"
40094, -- "Urine Out Condom Cath"
40715, -- "Urine Out Suprapubic"
40473, -- "Urine Out IleoConduit"
40085, -- "Urine Out Incontinent"
40057, -- "Urine Out Rt Nephrostomy"
40056, -- "Urine Out Lt Nephrostomy"
40405, -- "Urine Out Other"
40428, -- "Urine Out Straight Cath"
40086,--	Urine Out Incontinent
40096, -- "Urine Out Ureteral Stent #1"
40651, -- "Urine Out Ureteral Stent #2"

-- these are the most frequently occurring urine output observations in MetaVision
226559, -- "Foley"
226560, -- "Void"
226561, -- "Condom Cath"
226584, -- "Ileoconduit"
226563, -- "Suprapubic"
226564, -- "R Nephrostomy"
226565, -- "L Nephrostomy"
226567, --	Straight Cath
226557, -- R Ureteral Stent
226558, -- L Ureteral Stent
227488, -- GU Irrigant Volume In
227489  -- GU Irrigant/Urine Volume Out
)
group by ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID
order by ie.SUBJECT_ID, ie.HADM_ID, ie.ICUSTAY_ID;

#


####  VENTILATION SETTINGS

## [TO BE ADDED]

#### VENTILATION DURATION

CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_ventdurations` as
with vd0 as
(
  select
    icustay_id
    -- this carries over the previous charttime which had a mechanical ventilation event
    , case
        when MechVent=1 then
          LAG(CHARTTIME, 1) OVER (partition by icustay_id, MechVent order by charttime)
        else null
      end as charttime_lag
    , charttime
    , MechVent
    , OxygenTherapy
    , Extubated
    , SelfExtubated
  from `bgse-dsc.MIMIC3_V1_4.VK_ventsettings`
)
, vd1 as
(
  select
      icustay_id
      , charttime_lag
      , charttime
      , MechVent
      , OxygenTherapy
      , Extubated
      , SelfExtubated

      -- if this is a mechanical ventilation event, we calculate the time since the last event
      , case
          -- if the current observation indicates mechanical ventilation is present
          -- calculate the time since the last vent event
          when MechVent=1 then
           -- CHARTTIME - charttime_lag
           TIMESTAMP_DIFF(TIMESTAMP(CHARTTIME), TIMESTAMP(charttime_lag),HOUR)
          else null
        end as ventduration

      , LAG(Extubated,1)
      OVER
      (
      partition by icustay_id, case when MechVent=1 or Extubated=1 then 1 else 0 end
      order by charttime
      ) as ExtubatedLag

      -- now we determine if the current mech vent event is a "new", i.e. they've just been intubated
      , case
        -- if there is an extubation flag, we mark any subsequent ventilation as a new ventilation event
          --when Extubated = 1 then 0 -- extubation is *not* a new ventilation event, the *subsequent* row is
          when
            LAG(Extubated,1)
            OVER
            (
            partition by icustay_id, case when MechVent=1 or Extubated=1 then 1 else 0 end
            order by charttime
            )
            = 1 then 1
          -- if patient has initiated oxygen therapy, and is not currently vented, start a newvent
          when MechVent = 0 and OxygenTherapy = 1 then 1
            -- if there is less than 8 hours between vent settings, we do not treat this as a new ventilation event
          -- TIMESTAMP_DIFF(timestamp_1, timestamp_2, MINUTE)
          --when TIMESTAMP_DIFF(TIMESTAMP(CHARTTIME) - TIMESTAMP(charttime_lag)) > interval 8 hour
          when TIMESTAMP_DIFF(TIMESTAMP(CHARTTIME),TIMESTAMP(charttime_lag),HOUR) > 8
            then 1
        else 0
        end as newvent
  -- use the staging table with only vent settings from chart events
  FROM vd0 `bgse-dsc.MIMIC3_V1_4.VK_ventsettings`
)
, vd2 as
(
  select vd1.*
  -- create a cumulative sum of the instances of new ventilation
  -- this results in a monotonic integer assigned to each instance of ventilation
  , case when MechVent=1 or Extubated = 1 then
      SUM( newvent )
      OVER ( partition by icustay_id order by charttime )
    else null end
    as ventnum
  --- now we convert CHARTTIME of ventilator settings into durations
  from vd1
)
-- create the durations for each mechanical ventilation instance
select icustay_id
  -- regenerate ventnum so it's sequential
  , ROW_NUMBER() over (partition by icustay_id) as ventnum
  , min(charttime) as starttime
  , max(charttime) as endtime
  --, extract(epoch from max(charttime)-min(charttime))/60/60 AS duration_hours
  -- max(TIMESTAMP_ADD(TIMESTAMP(charttime), INTERVAL 4  HOUR))
  ,  (max(UNIX_SECONDS(TIMESTAMP(charttime))) - min(UNIX_SECONDS(TIMESTAMP(charttime))))/60/60 AS duration_hours
from vd2
group by icustay_id
having min(charttime) != max(charttime)
-- patient had to be mechanically ventilated at least once
-- i.e. max(mechvent) should be 1
-- this excludes a frequent situation of NIV/oxygen before intub
-- in these cases, ventnum=0 and max(mechvent)=0, so they are ignored
and max(mechvent) = 1
order by icustay_id;


CREATE TABLE `bgse-dsc.MIMIC3_V1_4.VK_ventsettings` AS
select
  icustay_id, charttime
  -- case statement determining whether it is an instance of mech vent
  , max(
    case
      when itemid is null or value is null then 0 -- can't have null values
      when itemid = 720 and value != 'Other/Remarks' THEN 1  -- VentTypeRecorded
      when itemid = 223848 and value != 'Other' THEN 1
      when itemid = 223849 then 1 -- ventilator mode
      when itemid = 467 and value = 'Ventilator' THEN 1 -- O2 delivery device == ventilator
      when itemid in
        (
        445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
        , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
        , 218,436,535,444,459,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean/Neg insp force ("RespPressure")
        , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
        , 543 -- PlateauPressure
        , 5865,5866,224707,224709,224705,224706 -- APRV pressure
        , 60,437,505,506,686,220339,224700 -- PEEP
        , 3459 -- high pressure relief
        , 501,502,503,224702 -- PCV
        , 223,667,668,669,670,671,672 -- TCPCV
        , 224701 -- PSVlevel
        )
        THEN 1
      else 0
    end
    ) as MechVent
    , max(
      case
        -- initiation of oxygen therapy indicates the ventilation has ended
        when itemid = 226732 and value in
        (
          'Nasal cannula', -- 153714 observations
          'Face tent', -- 24601 observations
          'Aerosol-cool', -- 24560 observations
          'Trach mask ', -- 16435 observations
          'High flow neb', -- 10785 observations
          'Non-rebreather', -- 5182 observations
          'Venti mask ', -- 1947 observations
          'Medium conc mask ', -- 1888 observations
          'T-piece', -- 1135 observations
          'High flow nasal cannula', -- 925 observations
          'Ultrasonic neb', -- 9 observations
          'Vapomist' -- 3 observations
        ) then 1
        when itemid = 467 and value in
        (
          'Cannula', -- 278252 observations
          'Nasal Cannula', -- 248299 observations
          -- 'None', -- 95498 observations
          'Face Tent', -- 35766 observations
          'Aerosol-Cool', -- 33919 observations
          'Trach Mask', -- 32655 observations
          'Hi Flow Neb', -- 14070 observations
          'Non-Rebreather', -- 10856 observations
          'Venti Mask', -- 4279 observations
          'Medium Conc Mask', -- 2114 observations
          'Vapotherm', -- 1655 observations
          'T-Piece', -- 779 observations
          'Hood', -- 670 observations
          'Hut', -- 150 observations
          'TranstrachealCat', -- 78 observations
          'Heated Neb', -- 37 observations
          'Ultrasonic Neb' -- 2 observations
        ) then 1
      else 0
      end
    ) as OxygenTherapy
    , max(
      case when itemid is null or value is null then 0
        -- extubated indicates ventilation event has ended
        when itemid = 640 and value = 'Extubated' then 1
        when itemid = 640 and value = 'Self Extubation' then 1
      else 0
      end
      )
      as Extubated
    , max(
      case when itemid is null or value is null then 0
        when itemid = 640 and value = 'Self Extubation' then 1
      else 0
      end
      )
      as SelfExtubated
from bgse-dsc.MIMIC3_V1_4.CHARTEVENTS ce
where ce.value <> ''
-- exclude rows marked as error
and ce.error != 1
and itemid in
(
    -- the below are settings used to indicate ventilation
      720, 223849 -- vent mode
    , 223848 -- vent type
    , 445, 448, 449, 450, 1340, 1486, 1600, 224687 -- minute volume
    , 639, 654, 681, 682, 683, 684,224685,224684,224686 -- tidal volume
    , 218,436,535,444,224697,224695,224696,224746,224747 -- High/Low/Peak/Mean ("RespPressure")
    , 221,1,1211,1655,2000,226873,224738,224419,224750,227187 -- Insp pressure
    , 543 -- PlateauPressure
    , 5865,5866,224707,224709,224705,224706 -- APRV pressure
    , 60,437,505,506,686,220339,224700 -- PEEP
    , 3459 -- high pressure relief
    , 501,502,503,224702 -- PCV
    , 223,667,668,669,670,671,672 -- TCPCV
    , 224701 -- PSVlevel

    -- the below are settings used to indicate extubation
    , 640 -- extubated

    -- the below indicate oxygen/NIV, i.e. the end of a mechanical vent event
    , 468 -- O2 Delivery Device#2
    , 469 -- O2 Delivery Mode
    , 470 -- O2 Flow (lpm)
    , 471 -- O2 Flow (lpm) #2
    , 227287 -- O2 Flow (additional cannula)
    , 226732 -- O2 Delivery Device(s)
    , 223834 -- O2 Flow

    -- used in both oxygen + vent calculation
    , 467 -- O2 Delivery Device
)
group by icustay_id, charttime
UNION ALL
-- add in the extubation flags from procedureevents_mv
-- note that we only need the start time for the extubation
-- (extubation is always charted as ending 1 minute after it started)
select
  icustay_id, starttime as charttime
  , 0 as MechVent
  , 0 as OxygenTherapy
  , 1 as Extubated
  , case when itemid = 225468 then 1 else 0 end as SelfExtubated
from bgse-dsc.MIMIC3_V1_4.PROCEDUREEVENTS_MV
where itemid in
(
  227194 -- "Extubation"
, 225468 -- "Unplanned Extubation (patient-initiated)"
, 225477 -- "Unplanned Extubation (non-patient initiated)"
);