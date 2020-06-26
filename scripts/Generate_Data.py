from importlib.machinery import SourceFileLoader
import algosdk
import numpy as np
import pandas as pd
import datetime
import os
import re
import json
import joblib

# load custome module from path.
covid19_WebScrapes = SourceFileLoader("covid19_WebScrapes", "./scripts/covid19_WebScrapes.py").load_module()
merge_prep_data = SourceFileLoader("merge_data", "./scripts/merge_prep_data.py").load_module()



# Generate the time series cases data.
print('Generating the Cases data')
covid19_county_level = covid19_WebScrapes.TestingData_Scraper()
Testing_DF = covid19_county_level.Get_Final_DF(Impute = False) #Marked as False, I think logic is broken


# Generage the algorand survey data.
'''
API_KEY=str(np.loadtxt('local_var.txt',dtype=str))[8:]
alg_tx = covid19_WebScrapes.Algorand_Scrape(API_KEY)
Survey_DF = alg_tx.Convert_to_DF()
Survey_DF_trim = Survey_DF[['gc','gr','gzp','ga','gs','sz','tz','tt','tr','mz','qz','q1','q2','q3','q4','ql']]
Survey_DF_trim.columns = ['Country','Region','Zip','AgeGroup','Gender','Symptomatic',
                          'Tested','Tested_Attempt','Test_Result','Received_Care','Quarantined',
                          'Q_Symptoms','Q_Voluntary','Q_Personal','Q_General','Left_Quarantine']
'''


# Generate the area data
print('Generating the Area data')
wiki_scraper = covid19_WebScrapes.Wiki_Scrape()
county_areas = wiki_scraper.Scrape_Counties()
County_Areas = pd.DataFrame(county_areas,columns=['State','County_FIPS','County','Area (sqmi)'])
County_Areas['Area (sqmi)'] = County_Areas['Area (sqmi)'] .apply(lambda x : str(x).replace(',',''))
County_Areas['Area (sqmi)'] = County_Areas['Area (sqmi)'].astype(float)



# Generate the Google Mobility time series data 
print('Generating the Google Mobility Data')
google = covid19_WebScrapes.Alphabet_Scrape_V2()
google_df = google.get_Data(country='United States',country_only=False,state_only=False) #pulls county info only

# Generate the Orders Data
print('Generating the Orders Data')
orders = covid19_WebScrapes.OrdersScrape()
orders_df = orders.getzip()


# Clean and merge data.
print('Cleaning all data')
data_cleaner = covid19_WebScrapes.Clean_Data('./manually_pulled/FIPS_Codes_USDA.csv',
                                             './manually_pulled/new_state_mapping.txt')
area_data_cleaned = data_cleaner.Clean_Area_Data(County_Areas)
test_data_cleaned = data_cleaner.Clean_Cases_Data(Testing_DF)
google_data_cleaned = data_cleaner.Clean_Loc_Data(google_df)
orders_data_cleaned = data_cleaner.Clean_Orders_Data(orders_df)

folder_name = datetime.datetime.strftime(datetime.datetime.today(),'%d%b%y')
os.mkdir('./Processed_Data/'+folder_name)

print('Writing out cleaned data')
test_data_cleaned.to_csv('./Processed_Data/'+folder_name+'/CountyLevel_Cases_Cleaned.csv',index=False)
#Survey_DF_trim.to_csv('Processed_Data/'+folder_name+'/Survey_Data.csv',index=False)
area_data_cleaned.to_csv('./Processed_Data/'+folder_name+'/CountyLevel_Areas_Cleaned.csv',index=False)
google_data_cleaned.to_csv('./Processed_Data/'+folder_name+'/CountyLevel_Google_LocData_Cleaned.csv',index=False)
orders_data_cleaned.to_csv('Processed_Data/'+folder_name+'/StateLevel_Orders_Cleaned.csv',index=False)

print('Merging all data')
data_merger = merge_prep_data.Merge_Data()
merged_census_data = data_merger.Merge_Census_Data()
merged_scraped_data = data_merger.Merge_Scraped_Data()
final_merge = data_merger.MERGE_ALL(merged_scraped_data,merged_census_data)

cols_remove = ['County_FIPS','County','UID','iso2','iso3','code3','Province_State',
               'Country_Region','Combined_Key','country_region_code','country_region','sub_region_1',
               'sub_region_2','date','State_fip','Unnamed: 0','county','state_FIPS',
              'county_fips','census_fips_code']

final_merge.drop(cols_remove,axis=1,inplace=True)

print('Engineering Features')
engineer_feats = merge_prep_data.Engineer_Feats(datatype_write_loc='./Merged_Data/data_types.json')

DF_out = engineer_feats.Apply_Logic(final_merge)

cols_keep = ['FIPS','state','Admin2','Lat','Long_','Date',
             'Positive','Deaths','Positive_Cases_PopNormed','Deaths_PopNormed',
             'New_Positive_Cases_PopNormed_Lagged',
             'New_Positive_Cases_PopNormed',
             'Area (sqmi)','population','Proximity','Population_Density','Percent_in_Workforce',
             'Households_per_SqMile',
             'retail_and_recreation_percent_change_from_baseline',
             'grocery_and_pharmacy_percent_change_from_baseline',
             'parks_percent_change_from_baseline',
             'transit_stations_percent_change_from_baseline',
             'workplaces_percent_change_from_baseline',
             'residential_percent_change_from_baseline',
             'Mass gathering restrictions', 'Initial business closure',
             'Educational facilities closed', 'Non-essential services closed',
             'Stay at home order', 'Travel severely limited',
             '%_family_households', '%_single_male_households',
             '%_single_female_households', '%_living_alone',
             'total_household_income', 'household_income_less_than_25_years',
             'household_income_25_to_45_years', 'household_income_45_to_65_years',
             'household_income_65_and_older',
             '%_households_with_earnings_last12',
             '%_households_with_no_earnings_last12', '%_in_poverty',
             '%_in_poverty_18_to_59', '%_in_poverty_60_to_74',
             '%_in_poverty_75_to_85',
             '%_white', '%_black',
             '$_other_race', 
             '%_male', '%_female', 
             '%_male_pop_greater_than_60','%_female_pop_greater_than_60',
             '%_workers_less_than_15_to_work', '%_workers_15_to_45_to_work',
             '%_workers_greater_than_45_to_work',
             '%_drive_alone_to_work', '%_carpool_to_work',
             '%_public_transit_to_work', '%_bus_trolley_to_work', '%_walked_to_work',
             '%_cab_other_means_of_transportation_to_work','jail_incarceration_rate_per_100k']

print('Writing out Merged Data (probably in chunks)')
DF_write = DF_out[cols_keep]

engineer_feats.write_out_json(DF_write)

folder_name = datetime.datetime.strftime(datetime.datetime.today(),'%d%b%y')
if not os.path.exists('./Merged_Data/'+folder_name) :
    os.mkdir('./Merged_Data/'+folder_name)
    
memory_use = DF_write.memory_usage(deep=True,index=False).sum()    

if memory_use > 1e8 :
    print('splitting dataframes for Github push purposes')
    break_val = int((DF_write.shape[0] * 45000000)/memory_use)
    for i in range(int(np.ceil(DF_write.shape[0]/break_val))) :
        name_write = './Merged_Data/'+folder_name+'/Final_Merged_Pt{}.csv'.format(i+1)
        chunk_write = DF_write.iloc[break_val*i : break_val*(i+1)]
        chunk_write.to_csv(name_write,index=False)
else :
    DF_write.to_csv('./Merged_Data/'+folder_name+'/Final_Merged.csv',index=False) #write out csv file
