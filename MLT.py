import pandas as pd
import sys
from ast import literal_eval
import warnings
import re
import numpy as np
import time
import logging
import ast
import json
import os

sys.path.append("/home/azureuser/Operations/619/DB_TRANSFORMATIONS_PROD/")
warnings.filterwarnings("ignore")

from Helper_Scripts.HCC_helper import preparatory_files_path
from Helper_Scripts.HCC_helper import lookups_files_path
from Helper_Scripts.config_prep_emp import chronic_dict_df_new
from Helper_Scripts.config_prep_links import HCC_output
from Helper_Scripts.config_prep_links import ddi_output
from Helper_Scripts.config_prep_links import eligible_caregap_output1
from Helper_Scripts.config_prep_links import adherence_output
from Helper_Scripts.config_prep_links import onsetmodel_output
from Helper_Scripts.config_prep_links import disease_names
from Helper_Scripts.config_prep_emp import diagnosis_dictionary_df
from Helper_Scripts.config_prep_emp import trigger_concat_df1
from Helper_Scripts.config_prep_emp import pcp_lookup
from Helper_Scripts.MLT_Helper import hcc_risk_dict
from Helper_Scripts.MLT_Helper import rx_df_return
from Helper_Scripts.MLT_Helper import med_df_return
from Helper_Scripts.config_prep_emp import hedis_lookup_new
from Helper_Scripts.config_prep_emp import output_path
from Helper_Scripts.config_prep_emp import log_path

#Logging Decleration
log_file = f"{log_path}Transformation/member_level_transformation/Benchmarking.log"
error_log_file = f"{log_path}Transformation/member_level_transformation/Error.log"

if not os.path.isfile(log_file):
    open(log_file, 'w').close()

if not os.path.isfile(error_log_file):
    open(error_log_file, 'w').close()


logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
error_logger = logging.getLogger("error_logger")
error_handler = logging.FileHandler(error_log_file)
error_handler.setLevel(logging.ERROR)  # Set the log level to ERROR for error messages
error_formatter = logging.Formatter('%(asctime)s ERROR: %(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)

start_time = time.time()

# grp_claim_mnth_year = pd.read_parquet(f"{preparatory_files_path}claim_set.parquet")
grp_claim_mnth_year = pd.read_parquet(preparatory_files_path+"claim_set.parquet")
grp_claim_mnth_year_rx = pd.read_parquet(preparatory_files_path+"claim_set_rx.parquet")

chronic_dict_df1=chronic_dict_df_new()



chronic1=chronic_dict_df1[["code","desc","chronic_disease_1","group_name1","level1_desc","level2_desc"]].rename(columns={"chronic_disease_1":"disease","group_name1":"group_name"})
chronic2=chronic_dict_df1[chronic_dict_df1["chronic_disease_2"].notnull()] [["code","desc","chronic_disease_2","group_name2","level1_desc","level2_desc"]].rename(columns={"chronic_disease_2":"disease","group_name2":"group_name"})
chronic3=chronic_dict_df1[chronic_dict_df1["chronic_disease_3"].notnull()][["code","desc","chronic_disease_3","group_name3","level1_desc","level2_desc"]].rename(columns={"chronic_disease_3":"disease","group_name3":"group_name"})
chronic_dict=pd.concat([chronic1,chronic2,chronic3])
chronic_dict["disease"]=chronic_dict["disease"].str.replace("Alzheimerâ€™s", "Alzheimer").str.replace("Parkinsonâ€™s","Parkinson")
chronic_dict=chronic_dict.astype(str)

def fetch_adherence():
    try:
        adherence_df = adherence_output()
        adherence_df['service_date'] = pd.to_datetime(adherence_df['service_date'])
        adherence_df=adherence_df.rename(columns={'person_id':'member_unique_id'})
        return adherence_df
    except Exception as e:
        print(e)

def  fetch_cargap():
    try:
        elig_caregap_df = eligible_caregap_output1().drop_duplicates(subset=['eg_nid', 'plan_year', 'member_unique_id', 'member_gender', 'rule_id','flag', 'pcp_met', 'provider_met', 'admission_met'])
        # print(elig_caregap_df.columns,elig_caregap_df.shape)
        # import sys
        # sys.exit()

        hedis_lookup_df_new=hedis_lookup_new()
        if len(elig_caregap_df)>0:
            print(elig_caregap_df["rule_id"].unique())
            
            elig_caregap_df = elig_caregap_df.merge(hedis_lookup_df_new[["rule_id", "measure", "sub_measure", "icd10_desc","individual_measures","url", "chronic_disease","self_imposed_flag"]], on =["rule_id"], how ="inner")
        else:
            elig_caregap_df=pd.DataFrame(columns=['eg_nid', 'service_year', 'member_unique_id', 'member_gender', 'rule_id','flag', 'pcp_met', 'provider_met', 'admission_met',"sub_measure","individual_measures","url", "chronic_disease","self_imposed_flag"])


        elig_caregap_df["sub_measure"] = elig_caregap_df["sub_measure"].astype(str).str.replace("�","")

        elig_caregap_df["chronic_disease"] = elig_caregap_df["chronic_disease"].apply(lambda x: [] if str(x)=="nan" else [x])
    # adher_output_df=adherence_output()

        for i in elig_caregap_df.columns:
            if i != "chronic_disease":
                elig_caregap_df[i]=elig_caregap_df[i].astype(str)
        elig_caregap_df["flag"]=elig_caregap_df["flag"].replace("Met","met")
        elig_caregap_df['service_date'] = pd.to_datetime(elig_caregap_df['plan_year'].str[11:])
        print(elig_caregap_df.shape)
        if len(elig_caregap_df)>0:
            elig_caregap_df_ = elig_caregap_df.astype(str).rename(columns={"group_number":"eg_nid", "person_id":"member_unique_id"}).groupby(["eg_nid","plan_year",
                                                                        "member_unique_id",'service_date', 'rule_id']).apply(lambda x:{"rule_id":x["rule_id"].iloc[0],
                                                                                                                        "provider_npi":x["pcp_met"].iloc[0],
                                                                                                                    "provider_speciality":x["provider_met"].iloc[0],
                                                                                                                    "admission_met":x["admission_met"].iloc[0],
                                                                                                                        "flag":x["flag"].iloc[0], 
                                                                                                                        "measure":x["measure"].iloc[0],
                                                                                                                        "sub_measure":x["sub_measure"].iloc[0],
                                                                                                                        "individual_measures":x["individual_measures"].iloc[0],
                                                                                                                        "chronic_disease":x["chronic_disease"].iloc[0],
                                                                                                                        "measure_url":x["url"].iloc[0]
                                                                                                                                                }).reset_index().rename(columns = {0:"caregap_details"})
        # display(elig_caregap_df_)
            elig_caregap_df_ = elig_caregap_df_.groupby(["eg_nid","plan_year",'service_date',"member_unique_id"]).agg({"caregap_details":lambda x:x.to_list()}).reset_index()
            elig_caregap_df_['service_date'] = pd.to_datetime( elig_caregap_df_['service_date'] )
        else:
            elig_caregap_df_=pd.DataFrame(columns=["eg_nid","service_date","plan_year","member_unique_id","caregap_details"])

        return elig_caregap_df_
    except Exception as e:
        print(e)

adherence = adherence_output()
adherence['service_date'] = pd.to_datetime(adherence['service_date'])

current_plan_start_date = '2025-03-01'
current_plan_end_date = '2026-02-28'

def assign_plan_year(service_from_date,service_to_date,year_range):
    
    service_from_date= pd.to_datetime(service_from_date)
    service_to_date = pd.to_datetime(service_to_date)
    for year in year_range:
        if ((service_from_date>= pd.to_datetime(str(int(year))+current_plan_start_date[4:])) & (service_to_date<= pd.to_datetime(str(int(year)+1)+current_plan_end_date[4:]))):
            
            return str(int(year))+current_plan_start_date[4:] +"_"+str(int(year)+1)+current_plan_end_date[4:]
    print(str(int(current_plan_start_date[:4]))+current_plan_start_date[4:] +"_"+str(int(current_plan_start_date[:4]))+current_plan_end_date[4:])    
    return str(int(current_plan_start_date[:4]))+current_plan_start_date[4:] +"_"+str(int(current_plan_start_date[:4]))+current_plan_end_date[4:]

def plan_year_processing(df,min_claim_date,max_claim_date,duration='plan_year'):
    try:
        df['service_from_date']=pd.to_datetime(df['service_from_date'])
        # print(df)
        if duration=='plan_year':
            min_year=pd.to_datetime(df['service_from_date']).dt.year.min()-1
            max_year=pd.to_datetime(df['service_from_date']).dt.year.max()+1
            print(min_year,max_year)
            start_range_date=str(min_year)+current_plan_start_date[4:]
            end_range_date=str(max_year)+current_plan_start_date[4:]
            print(start_range_date)
            year_range=pd.date_range(start=start_range_date, end=end_range_date, freq=pd.DateOffset(months=12, day=1))
            # print(year_range)
            df['plan_year']=df.apply(lambda x : assign_plan_year(x['service_from_date'],year_range),axis=1)
        else:
            print(duration)
            # max_claim_date = pd.to_datetime(df['service_from_date']).max().strftime('%Y-%m-%d')
            # min_claim_date = pd.to_datetime(df['service_from_date']).min().strftime('%Y-%m-%d')
            
            period=len(pd.date_range( start=min_claim_date, end=max_claim_date, freq=pd.DateOffset(months=int(duration))))+1
            max_day=int(max_claim_date.split("-")[-1])
            # period=len(pd.to_datetime(df['service_from_date']).dt.year.unique())+1
            year_range=pd.date_range( end=max_claim_date, periods=period, freq=pd.DateOffset(months=int(duration), day=max_day))
            # print(year_range,"year_range")
            df['plan_year_latest']=df.apply(lambda x : assign_duration_year(x['service_from_date'],year_range,int(duration)),axis=1)
            latest_plan_year=str(year_range[-2])[:10]+"_"+str(year_range[-1])[:10]
            print(latest_plan_year,"latest_plan_year")
            print(type(df))

        
        

        return latest_plan_year,df
    except Exception as e:
        print(e)
        return pd.DataFrame()
def assign_duration_year(fill_date,year_range,duration):
    try:
        for year in sorted(year_range):
            print(year)
            # print(fill_date,year+pd.DateOffset(months=0-duration))
            if (fill_date>(year+pd.DateOffset(months=0-duration))) and (fill_date<=year):
                plan_year=(year+pd.DateOffset(months=0-duration)).strftime('%Y-%m-%d')+"_"+year.strftime('%Y-%m-%d')
                # print(plan_year,"latest1")
                return plan_year
            # else:
        plan_year= (year_range[-1].strftime('%Y-%m-%d')+"_"+(year_range[-1]+pd.DateOffset(months=0+duration)).strftime('%Y-%m-%d'))
        # print(plan_year)
        # print(plan_year,"latest2")
        return plan_year
    except Exception as e:
        print(e)
        return "-"
def trigger_patients():
        
        med_input_df_ = med_input_df_1
        med_input_df = med_input_df_.assign(claim_type="medical")
        claims = med_input_df
        diag_columns = [i for i in claims.columns if "condition" in i and "source_value" in i]
        trigger_patients_list = {}
        claims=claims[claims["plan_year_latest"]== latest_plan_year]
        for i in claims["plan_year_latest"].unique():
            for o in diag_columns:
                
                trigger_patients_list[i+o] = claims[(claims["plan_year_latest"]==i) & (claims["claim_type"]=="medical")][["group_number", "person_id","plan_year_latest"]+[o]].rename(columns = {o:"ICD_code"}).drop_duplicates()
        trigger_pats_df = pd.concat(trigger_patients_list.values())
        trigger_pats_df["ICD_code_3"] = trigger_pats_df["ICD_code"].astype(str).str[:3]
        trigger_pats_df["ICD_code_4"] = trigger_pats_df["ICD_code"].astype(str).str[:4]
        trigger_pats_df["ICD_code_5"] = trigger_pats_df["ICD_code"].astype(str).str[:5]
        trigger_pats_df["ICD_code_6"] = trigger_pats_df["ICD_code"].astype(str).str[:6]
        
        trigger_lookup_df = trigger_concat_df1()
        diagnosis_dict_df=diagnosis_dictionary_df()
        print("trigger1",trigger_lookup_df.shape)
        trigger_lookup_df=trigger_lookup_df.merge(diagnosis_dict_df, left_on =["codes"], right_on = ["code"], how = "left").drop(columns=["level1_desc","level2_desc"]) 
        print("trigger2",trigger_lookup_df.shape)
        print(trigger_lookup_df.columns)
        trigger_pats_df_3 = trigger_pats_df.merge(trigger_lookup_df[trigger_lookup_df["codes"].apply(lambda x: len(x))==3], left_on = ["ICD_code_3"], right_on = ["codes"], how = "inner")
        trigger_pats_df_4 = trigger_pats_df.merge(trigger_lookup_df[trigger_lookup_df["codes"].apply(lambda x: len(x))==4], left_on = ["ICD_code_4"], right_on = ["codes"], how = "inner")
        trigger_pats_df_5 = trigger_pats_df.merge(trigger_lookup_df[trigger_lookup_df["codes"].apply(lambda x: len(x))==5], left_on = ["ICD_code_5"], right_on = ["codes"], how = "inner")
        trigger_pats_df_6 = trigger_pats_df.merge(trigger_lookup_df[trigger_lookup_df["codes"].apply(lambda x: len(x))==6], left_on = ["ICD_code_6"], right_on = ["codes"], how = "inner")
        
        trigger_pats_df = pd.concat([trigger_pats_df_3, trigger_pats_df_4, trigger_pats_df_5, trigger_pats_df_6]).drop_duplicates()
        trigger_1=trigger_pats_df
        if len(trigger_1)!=0:
            print(trigger_pats_df.columns)
            trigger_pats_df = trigger_pats_df.groupby(["group_number", "person_id","plan_year_latest", "ICD_code", "level1", 'level2',"desc"]).apply(lambda x:[{"trigger_code":x["ICD_code"].iloc[0], "level_1_desc":x["desc"].iloc[0], "level_2_desc":x["level2"].iloc[0]}]).reset_index().rename(columns = {0:"trigger_conditions"})
            trigger_pats_df = trigger_pats_df.groupby(["group_number", "person_id","plan_year_latest"]).agg({"trigger_conditions":lambda x:x.sum()})
            print("trigger1")
            trigger_pats_df_1=trigger_1.groupby(["group_number","plan_year_latest", "person_id"]).agg({"desc":lambda x:x.astype(str).nunique()}).reset_index().rename(columns={"desc":"trigger_count"})
            print("Trigger2")
            trigger_pats_df=trigger_pats_df.merge(trigger_pats_df_1,on=["group_number","plan_year_latest", "person_id"],how='left')
            return trigger_pats_df.rename(columns={"person_id":"member_unique_id"})
        else:
            return pd.DataFrame([])
def get_cost():
    input_df =pd.concat( [med_input_df_1,rx_input_df_])
    input_df=input_df[input_df["plan_year_latest"] == latest_plan_year]
    print(input_df.shape,"cost")
    cost = input_df.groupby(["group_number","plan_year_latest", "person_id"]).agg({"total_paid_amount":lambda x :x.astype(float).sum()}).reset_index()
    lvl_02 = cost.groupby(["group_number","plan_year_latest"])["total_paid_amount"].quantile([0.6, 0.8,0.95,0.98,0.99]).reset_index()
    for year in lvl_02["plan_year_latest"].unique():
        grpby_fields = ["group_number","plan_year_latest"]
        amt_t1 = lvl_02[(lvl_02["plan_year_latest"]==year) & (lvl_02["level_2"]==0.6)]["total_paid_amount"].iloc[0]
        amt_t2 = lvl_02[(lvl_02["plan_year_latest"]==year) & (lvl_02["level_2"]==0.8)]["total_paid_amount"].iloc[0]
        amt_t3 = lvl_02[(lvl_02["plan_year_latest"]==year) & (lvl_02["level_2"]==0.95)]["total_paid_amount"].iloc[0]
        amt_t5 = lvl_02[(lvl_02["plan_year_latest"]==year) & (lvl_02["level_2"]==0.99)]["total_paid_amount"].iloc[0]
        cost.loc[(cost["plan_year_latest"]== year)&(cost["total_paid_amount"]< amt_t1),"cost_id"]= '>60%'
        cost.loc[(cost["plan_year_latest"]== year)&(cost["total_paid_amount"]>= amt_t1) & (cost["total_paid_amount"] < amt_t2),"cost_id"]= '21%-60%'
        cost.loc[(cost["plan_year_latest"]== year)&(cost["total_paid_amount"]>= amt_t2) & (cost["total_paid_amount"] < amt_t3),"cost_id"]= '6%-20%'
        cost.loc[(cost["plan_year_latest"]== year)&(cost["total_paid_amount"]>=amt_t3) & (cost["total_paid_amount"] < amt_t5),"cost_id"]= '2%-5%'
        cost.loc[(cost["plan_year_latest"]== year)&(cost["total_paid_amount"]>= amt_t5)  ,"cost_id"]= '1%'
    cost=cost[["group_number","plan_year_latest", "person_id","cost_id"]].rename(columns={"person_id":"member_unique_id"})
        return cost   

def util_counts():
        
        med_input_df_ = med_input_df_1.assign(member_id=med_input_df_1["person_id"],discount=0, claim_category="medical")
        med_input_df_ = med_input_df_[['group_number', 'member_id', 'person_id', 'relationship_concept_id', 'relationship_desc', 'member_first_name', 'member_last_name', 'original_patient_first_name', 'original_patient_last_name', 'date_of_birth', 'gender_source_value', 'claim_id', 'claim_category', 'service_year', 'plan_year_latest','service_month', 'service_period_start_date', "admission_flag",'network_indicator', 'claim_status', "surgical_flag"]]
        a = ['group_number', 'member_id', 'person_id', 'relationship_concept_id', 'relationship_desc', 'member_first_name', 'member_last_name', 'original_patient_first_name', 'original_patient_last_name', 'date_of_birth', 'gender_source_value', 'claim_id', 'claim_category', 'service_year','plan_year_latest', 'service_month', 'service_period_start_date','network_indicator', 'claim_status']

        b = ['group_number', 'member_id', 'member_unique_id', 'member_relationship_code', 'member_relationship_desc', 'patient_first_name', 'patient_last_name', 'original_patient_first_name', 'original_patient_last_name', 'patient_date_of_birth', 'member_gender', 'claim_id', 'claim_type', 'service_year','plan_year_latest', 'month', 'service_from_date','network_indicator', 'claim_status']
        change_cols = {}
        for i in range(len(a)):
            change_cols[a[i]]=b[i]
        med_input_df_ = med_input_df_.rename(columns=change_cols)
        input_df = pd.concat([med_input_df_])
        ff = ["group_number","plan_year_latest","member_unique_id"]
        input_df=input_df[input_df["plan_year_latest"]==latest_plan_year]
        input_df_2=input_df.groupby(ff).apply(lambda x : (x[((x["admission_flag"].astype(str).str.upper().str.contains("OUT PATIENT"))& x["surgical_flag"].astype(str).str.upper().str.contains("SURGICAL")) | x["admission_flag"].astype(str).str.upper().str.contains("EMERGENCY") | x["admission_flag"].astype(str).str.upper().str.contains("URGENT CARE") | x["admission_flag"].astype(str).str.upper().str.contains("IN PATIENT")]["claim_id"].nunique())). reset_index().rename(columns ={0:"total_utilized"})
            
        input_df_1 = input_df.groupby(ff).apply(lambda x:{"outpatient_utilization": x[x["admission_flag"].astype(str).str.upper().str.contains("OUT PATIENT")]["claim_id"].nunique(),
                                                        "inpatient_utilization": x[x["admission_flag"].astype(str).str.upper().str.contains("IN PATIENT")]["claim_id"].nunique(),
                                                        "ambulatory_surgical_utilization": x[x["admission_flag"].astype(str).str.upper().str.contains("AMBULATORY")]["claim_id"].nunique(),
                                                        
                                                        "emergency_utilization": x[x["admission_flag"].astype(str).str.upper().str.contains("EMERGENCY")]["claim_id"].nunique(),
                                                        "urgent_care_utilization": x[x["admission_flag"].astype(str).str.upper().str.contains("URGENT CARE")]["claim_id"].nunique()}).reset_index().rename(columns ={0:"utilization_counts"})
        input_df_1=input_df_1.merge(input_df_2, on = ff,how="left")
        
        return input_df_1.rename(columns={"person_id":"member_unique_id"}) 
          
def get_prevalence():
    med_input_df=med_input_df_1[med_input_df_1["plan_year_latest"]==latest_plan_year]
        #Prevalence
    diag_columns = [i for i in med_input_df.columns if "condition_" in i and "source_value" in i]

    chronic_patients_list = {}
    
    for i in med_input_df["plan_year_latest"].unique():
            for o in diag_columns:
                chronic_patients_list[i+o] = med_input_df[(med_input_df["plan_year_latest"]==i)][["group_number", "person_id","plan_year_latest"]+[o]].rename(columns = {o:"ICD_code"}).rename(columns = {o:"ICD_code"}).drop_duplicates()
                
    if len(chronic_patients_list.values())>0:
        chronic_pats_df_1 = pd.concat(chronic_patients_list.values())
    else:
        chronic_pats_df_1 = pd.DataFrame([], columns = ["group_number", "person_id","plan_year_latest", 'ICD_code'])
    chronic_pats_df = chronic_pats_df_1.merge(chronic_dict, left_on = ["ICD_code"],right_on="code", how = "inner")


    chronic_pats_df_ = chronic_pats_df[["group_number","plan_year_latest", "person_id"]].drop_duplicates()
    chronic_pats_df = chronic_pats_df[["group_number","plan_year_latest", "person_id", "disease"]].drop_duplicates()
    
    non_chronic_mem_dic = {}
    for year in chronic_pats_df_["plan_year_latest"].unique():
        mem_list = list(chronic_pats_df_[chronic_pats_df_["plan_year_latest"]==year]["person_id"].unique())
        non_chronic_mem = med_input_df[(med_input_df["plan_year_latest"]==year)  & (~med_input_df["person_id"].isin(mem_list))]
        non_chronic_mem_dic[year] = non_chronic_mem[["group_number","plan_year_latest", "person_id"]].drop_duplicates()
        
    if len(non_chronic_mem_dic.values())>0:
        non_chronic_mem_df = pd.concat(non_chronic_mem_dic.values()).assign(disease = 0)
    else:
        non_chronic_mem_df = pd.DataFrame([], columns = ["group_number","plan_year_latest", "person_id"])
    chronic_pats_df = chronic_pats_df.groupby(["group_number","plan_year_latest", "person_id"]).agg({"disease":lambda x: len([i for i in list(x.unique()) if i !=""])}).reset_index()

    lvl_04 = pd.concat([chronic_pats_df, non_chronic_mem_df])
    lvl_04["risk"]=""
    lvl_04["risk"][lvl_04["disease"] >= 6] = "Tier 1"
    lvl_04["risk"][(lvl_04["disease"] == 5) |( lvl_04["disease"] == 4)] = "Tier 2"
    lvl_04["risk"][(lvl_04["disease"] == 3) |( lvl_04["disease"] == 2)] = "Tier 3"
    lvl_04["risk"][lvl_04["disease"] == 1] = "Tier 4"
    lvl_04["risk"][lvl_04["disease"] == 0] = "Tier 5"
    lvl_04=lvl_04.rename(columns={"person_id":"member_unique_id"})
    return lvl_04.rename(columns = {"risk":"prevalence_tier","disease":"disease_count"})

 
elig_caregap_df_=fetch_cargap()
adherence=fetch_adherence()

def memblvl_run(med_input_df_1, rx_input_df_, emp_id,  ind):
    try:
        def filter_adherence(data):
            try:
                # print(data)
                start_date=pd.to_datetime(data['plan_year'][:10])
                end_date=pd.to_datetime(data['plan_year'][11:])
                member_unique_id=data['member_unique_id']
                # print(start_date,end_date,member_unique_id)
                
                filtered_adh=adherence[(adherence['member_unique_id']==member_unique_id) & (adherence['service_date']>=start_date) & (adherence['service_date']<=end_date)]
                # adherence['adherence_ratio'] = adherence['adherence_ratio'].apply(ast.literal_eval)
                if len(filtered_adh)>0:
                    print(filtered_adh)
                    filtered_adh['adherence_ratio'] = filtered_adh['adherence_ratio'].apply(ast.literal_eval)
                    # print(filtered_adh['adherence_ratio'].sum())
                    return {'max_claim_date':filtered_adh['service_date'].astype(str).iloc[0],'data':filtered_adh['adherence_ratio'].sum()}
                
                return {'max_claim_date':'','data':[]}
            except Exception as e:
                print(e)
                return {'max_claim_date':'','data':[]}
        def filter_rules_engine(data):
            try:
                # print(data)
                start_date=pd.to_datetime(data['plan_year'][:10])
                end_date=pd.to_datetime(data['plan_year'][11:])
                member_unique_id=data['member_unique_id']
                # print(start_date,end_date,member_unique_id)
                
                filtered_caregap=elig_caregap_df_[(elig_caregap_df_['member_unique_id']==member_unique_id) & (elig_caregap_df_['service_date']>=start_date) & (elig_caregap_df_['service_date']<=end_date)]
                # adherence['adherence_ratio'] = adherence['adherence_ratio'].apply(ast.literal_eval)
                if len(filtered_caregap)>0:
                    # print(filtered_caregap)
                    # filtered_caregap['caregap_details'] = filtered_caregap['adherence_ratio'].apply(ast.literal_eval)
                    # print(filtered_caregap['caregap_details'].sum())
                    return {'max_claim_date':filtered_caregap['service_date'].astype(str).iloc[0],'data':filtered_caregap['caregap_details'].sum()}
                return {'max_claim_date':'','data':[]}
            except Exception as e:
                print(e)
                return {'max_claim_date':'','data':[]}
        med_input_df_ = med_input_df_1[['group_number','plan_year', 'person_id']]
        # print(med_input_df_.columns.to_list())
        rx_input_df = rx_input_df_[['group_number','plan_year', 'person_id']]
        # print(rx_input_df.columns.to_list())
        input_df = pd.concat([med_input_df_,rx_input_df]).astype(str).drop_duplicates().rename(columns={'person_id':'member_unique_id'})
        print("plan_year...............................",input_df['plan_year'])
        input_df['adherence_ratio'] =input_df[['member_unique_id','plan_year']].apply(filter_adherence,axis=1)
        
        input_df['caregap_details'] =input_df[['member_unique_id','plan_year']].apply(filter_rules_engine,axis=1)
        
        #input_df=input_df.drop(columns="plan_year")
        cost_df=get_cost()[["group_number","member_unique_id","cost_id"]]
        print("cost_df",cost_df.columns)
        prevalence_df=get_prevalence()[["group_number","member_unique_id","prevalence_tier"]]
        print("prevalence",prevalence_df.columns)
        trigger_df=trigger_patients()[["group_number","member_unique_id","trigger_count"]]
        print("Trigger",trigger_df.columns)
        util_df=util_counts()[["group_number","member_unique_id","utilization_counts"]]
        print("Util",util_df.columns)
        input_df=input_df.merge(cost_df,on=["group_number","member_unique_id"])
        input_df=input_df.merge(prevalence_df,on=["group_number","member_unique_id"])
        input_df=input_df.merge(trigger_df,on=["group_number","member_unique_id"])
        input_df=input_df.merge(util_df,on=["group_number","member_unique_id"])
        input_df["cost_id"]=input_df["cost_id"].apply(lambda d: d if isinstance(d, str) else "")
        input_df["prevalence_tier"]=input_df["prevalence_tier"].apply(lambda d: d if isinstance(d, str) else "")
        input_df["trigger_count"]=input_df["trigger_count"].apply(lambda d: d if isinstance(d, int) else 0)
        input_df["utilization_counts"]=input_df["utilization_counts"].apply(lambda d: [d] if isinstance([d], list) else [])

        print(input_df.columns)
        final_json = json.loads(input_df.to_json(orient = "records"))
        print("final_json")

        with open(f"{output_path}MLT_1/member_level_{emp_id}.json", "w+") as f:
            json.dump(final_json, f, indent=4)
        print(f"{output_path}MLT_1/member_level_{emp_id}.json", "written")
    except Exception as e:
        print(e)      



grp_claim_mnth_year["emp_id"] = grp_claim_mnth_year["emp_month_year"].apply(lambda x: x.split("_")[0])
grp_claim_mnth_year["sy"] = grp_claim_mnth_year["emp_month_year"].apply(lambda x: x.split("_")[1])
grp_claim_mnth_year = grp_claim_mnth_year.groupby(["emp_id", "sy"]).agg({"emp_month_year":lambda x:list(x.unique())}).reset_index()
grp_claim_mnth_year_rx["emp_id"] = grp_claim_mnth_year_rx["employer_month_year"].apply(lambda x: x.split("_")[0])
grp_claim_mnth_year_rx["sy"] = grp_claim_mnth_year_rx["employer_month_year"].apply(lambda x: x.split("_")[1])
grp_claim_mnth_year_rx = grp_claim_mnth_year_rx.groupby(["emp_id", "sy"]).agg({"employer_month_year":lambda x:list(x.unique())}).reset_index()

grp_claim_mnth_year.rename(columns={"emp_month_year": "employer_month_year"}, inplace=True)
combined_group_claim_month_year = pd.concat([grp_claim_mnth_year, grp_claim_mnth_year_rx]).reset_index()
combined_group_claim_month_year = combined_group_claim_month_year.drop(columns=["index"])
condensed_df = combined_group_claim_month_year.groupby('emp_id').agg({
    # 'emp_id': 'first',  # Keep the first 'emp_id' (or customize as needed)
    'employer_month_year': lambda x: sorted(list(set(sum(x, []))))  # Combine and deduplicate lists
}).reset_index()
# print(condensed_df)
for emp_id in condensed_df["emp_id"].unique():
    if emp_id !="ASD1":
        import glob
        # for old_file in glob.glob(f"{output_path}MLT/member_level_{emp_id}*"):
            # os.remove(old_file)
        # for sy in condensed_df[(condensed_df["emp_id"]==emp_id)]["sy"].unique():
        #     print(emp_id,sy)

        rx_input_df_s = []
        med_input_df_1s = []

        prefix_list = condensed_df[(condensed_df["emp_id"]==emp_id)]["employer_month_year"].iloc[0]
        # print(prefix_list)
        try:
            rx_input_df_1 = pd.concat([rx_df_return(prefix) for prefix in prefix_list])
            med_input_df_1_ = pd.concat([med_df_return(prefix) for prefix in prefix_list])
            med_date=med_input_df_1_
            med_date["service_period_start_date"]= pd.to_datetime(med_date["service_period_start_date"])
            rx_date=rx_input_df_1.rename(columns={"fill_date":"service_period_start_date"})
            rx_date["service_period_start_date"]=pd.to_datetime(rx_date["service_period_start_date"])
            min_date = pd.concat([med_date[['service_period_start_date']],rx_date[['service_period_start_date']]])['service_period_start_date'].min().strftime('%Y-%m-%d')

            max_date = pd.concat([med_date[['service_period_start_date']],rx_date[['service_period_start_date']]])['service_period_start_date'].max().strftime('%Y-%m-%d')
            print("min",min_date)
            print("max",max_date)

        except Exception as e:
            print(e)

        person_id_list = list(set(list(med_input_df_1_["person_id"].unique())+list(rx_input_df_1["person_id"].unique())))
        import numpy
        if len(person_id_list)>=2000:
            mem_split_list_1 = numpy.array_split(person_id_list,15)
        else:
            mem_split_list_1 = [person_id_list]
        for mem_list_i in range(0, len(mem_split_list_1)):
            med_input_df_1 = med_input_df_1_[med_input_df_1_["person_id"].isin(list(mem_split_list_1[mem_list_i]))]
            med_input_df_1['plan_year'] = med_input_df_1_.apply(lambda x : assign_plan_year(x['service_period_start_date'],x['service_period_end_date'],med_input_df_1_['service_year'].unique()),axis=1)
            med_input_df_1=med_input_df_1.rename(columns={"service_period_start_date":"service_from_date"})
            latest_plan_year,med_input_df_1=plan_year_processing(med_input_df_1,min_date,max_date,'12')
            med_input_df_1=med_input_df_1.rename(columns={"service_from_date":"service_period_start_date"})
            print(med_input_df_1["plan_year_latest"].unique(),"plan_year_latest")

            # print(med_input_df_1['service_year'].unique())
            # 'service_period_start_date', 'service_period_end_date'
            rx_input_df_ = rx_input_df_1[rx_input_df_1["person_id"].isin(list(mem_split_list_1[mem_list_i]))]
            rx_input_df_['plan_year'] = rx_input_df_.apply(lambda x : assign_plan_year(x['fill_date'],x['fill_date'],rx_input_df_['service_year'].unique()),axis=1)
            rx_input_df_=rx_input_df_.rename(columns={"fill_date":"service_from_date"})
            latest_plan_year,rx_input_df_=plan_year_processing(rx_input_df_,min_date,max_date,'12')
            rx_input_df_=rx_input_df_.rename(columns={"service_from_date":"fill_date"})
            # print(med_input_df_1['service_year'].unique())
            # print(rx_input_df_['plan_year'].unique())
            ind_val = mem_list_i
            # print(emp_id, ind_val,"Started")
            # print(med_input_df_1.shape,rx_input_df_.shape)
            if len(med_input_df_1)==0 or len(rx_input_df_)==0:

                pass
            else:
                memblvl_run(med_input_df_1 = med_input_df_1, rx_input_df_ = rx_input_df_, emp_id = emp_id, ind=ind_val)
            print(emp_id,  "Ended")

stop_time = time.time()
time_taken = float(stop_time - start_time) / 3600

logging.info(f'Total time taken: {time_taken:.2f} hours')