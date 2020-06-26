import pandas as pd
import numpy as np
import datetime
import os
import json


class Merge_Data :

    def __init__(self,date_folder=None,census_folder=None,jail_data=None) :

        if date_folder is None :
            date_folder = datetime.datetime.strftime(datetime.datetime.today(),'%d%b%y')
        folder_scrape = 'Processed_Data/'+date_folder
        if not os.path.exists(folder_scrape) :
            raise ValueError('Todays directory does not exist. Generate or manually update date_folder')

        self.area_data = folder_scrape + '/CountyLevel_Areas_Cleaned.csv'
        self.cases_data = folder_scrape + '/CountyLevel_Cases_Cleaned.csv'
        self.loc_data = folder_scrape + '/CountyLevel_Google_LocData_Cleaned.csv'
        self.orders_data = folder_scrape + '/StateLevel_Orders_Cleaned.csv'

        if census_folder is None :
            census_folder = 'manually_pulled/cleaned_census_data/'
        if not os.path.exists(census_folder) :
            raise ValueError('Census directory does not exist. Generate or manually update date_folder')
        files_out = []
        for f in os.listdir(census_folder) :
            files_out.append(census_folder+'/'+f)
        self.census_files = files_out

        if jail_data is None :
            jail_data = 'manually_pulled/jail_population.csv'
        if not os.path.exists(jail_data) :
            raise ValueError('Jail file does not exist. Generate or manually update date_folder')
        self.jail_data = jail_data

        self._Prep_Orders()

    def _preserve_fips(self,file,col_preserve,zfill_val) :

        df = pd.read_csv(file)
        df[col_preserve] = df[col_preserve].apply(lambda x : str(int(x)).zfill(zfill_val) \
            if not pd.isna(x) else np.nan)
        return df

    def _Prep_Orders(self) :
        orders = self._preserve_fips(self.orders_data,'State_fip',2)

        cols = []
        for col in orders.columns :
            if 'date' in col :
                col_trim = '_'.join(col.split('_')[:-2])
                if col_trim not in cols :
                    cols.append(col_trim)

        orders_dict = {}

        for _,v in orders.iterrows() :
            state = v['State_fip']
            to_add = {}
            for col in cols :
                to_add[col] = {}
                start = col + '_start_date'
                end = col + '_end_date'
                to_add[col]['start'] = v[start]
                to_add[col]['end'] = v[end]
            orders_dict[state] = to_add

        self.orders_dict = orders_dict

    def Merge_Scraped_Data(self) :

        area = self._preserve_fips(self.area_data,'FIPS',5)
        cases = self._preserve_fips(self.cases_data,'FIPS',5)
        loc_data = self._preserve_fips(self.loc_data,'census_fips_code',5)

        merged = area.merge(cases,how='right',on='FIPS')

        merged = merged.merge(loc_data,how='left',left_on=['Date','FIPS'],right_on=['date','census_fips_code']).reset_index(drop=True)

        merged['State_fip'] = merged['FIPS'].apply(lambda x : x[:2])

        res_out = {name:[] for name in self.orders_dict['01'].keys()}
        for _,v in merged.iterrows() :
            state_fip = v['State_fip'] 
            date = datetime.datetime.strptime(v['Date'],'%m/%d/%y')
            for key in self.orders_dict[state_fip].keys() :
                if pd.isna(self.orders_dict[state_fip][key]['start']) :
                    res_out[key].append(0)
                    continue
                start_date_key = datetime.datetime.strptime(self.orders_dict[state_fip][key]['start'],'%Y-%m-%d')
                if date >= start_date_key :
                    if pd.isna(self.orders_dict[state_fip][key]['end']) :
                        res_out[key].append(1)
                    else :
                        end_date_key = datetime.datetime.strptime(self.orders_dict[state_fip][key]['end'],'%Y-%m-%d')
                        if date < end_date_key :
                            res_out[key].append(1)
                        else :
                            res_out[key].append(0) 
                else :
                    res_out[key].append(0) 

        orders_out = pd.DataFrame(res_out)
        orders_out.columns = ['Travel severely limited','Stay at home order','Educational facilities closed',
                              'Mass gathering restrictions','Initial business closure',
                              'Non-essential services closed']
        orders_out = orders_out[['Mass gathering restrictions','Initial business closure',
                            'Educational facilities closed','Non-essential services closed',
                            'Stay at home order','Travel severely limited']] # Needed to match order prior to update.

        return merged.join(orders_out)

    def Merge_Census_Data(self) :
        
        merged_out = self._preserve_fips(self.census_files[0],'county_fips',5)

        for file_read in self.census_files[1:] :
            int_read = self._preserve_fips(file_read,'county_fips',5)
            cols_drop = []
            for col in int_read.columns :
                if col in merged_out.columns :
                    if col != 'county_fips' :
                        cols_drop.append(col)
            int_read.drop(cols_drop,axis=1,inplace=True)
            merged_out = merged_out.merge(int_read,how='outer',on='county_fips')

        return merged_out

    def MERGE_ALL(self,merged_scraped_data_use,merged_census_data_use) :

        jail = self._preserve_fips(self.jail_data,'fips',5)
        jail['FIPS'] = jail['fips']
        jail['Date'] = jail['date'].apply(lambda x : datetime.datetime.strftime(datetime.datetime.strptime(x,'%Y-%m-%d'),'%m/%d/%y'))


        merged_out = merged_scraped_data_use.merge(merged_census_data_use,how='left',left_on='FIPS',right_on='county_fips')

        merged_out = merged_out.merge(jail[['FIPS','Date','jail_incarceration_rate_per_100k']],how='left',
                      on=['FIPS','Date'])

        merged_out['jail_incarceration_rate_per_100k'] = merged_out['jail_incarceration_rate_per_100k'].fillna(0)

        merged_out = merged_out[~pd.isna(merged_out['Area (sqmi)'])]
 
        return merged_out[merged_out['Positive'] > 0].reset_index(drop=True)


class Engineer_Feats :

    def __init__(self,BigCity_DensityCutoff=2450,write_out_datatypes=True,datatype_write_loc=None) :

        self.cutoff = BigCity_DensityCutoff
        self.write_dtypes = write_out_datatypes

        if write_out_datatypes :
            if datatype_write_loc is None :
                raise ValueError('Need to input a location to write out datatypes file')
            else :
                self.loc_write_dtypes = datatype_write_loc

    def Apply_Logic(self,dataframe,stat_fill_method='nearest') :

        dataframe['Population_Density'] = dataframe['population'] / dataframe['Area (sqmi)']

        print('Adding Proximity Logic...')
        DF = self._Proximity_Logic(dataframe)

        print('Normalizing Stats , lagging features...')
        DF = self._Normalize_Stats(DF)

        print('Interpolating Data...')
        DF = self._Interpolate_Stats(DF,stat_fill_method)

        for col in DF.columns :
            if col == 'FIPS' :
                continue
            try :
                DF[col] = DF[col].astype(float)
            except :
                print("Couldn't convert {} to float".format(col))

        return DF
    
    def _Proximity_Logic(self,DF) :

        DF_FIPS_info = {fips:{} for fips in DF['FIPS'].unique()}

        for _,v in DF.iterrows() :
            if 'lat_long' not in DF_FIPS_info[v['FIPS']] :
                DF_FIPS_info[v['FIPS']]['lat_long'] = str(v['Lat'])+','+str(v['Long_'])
                DF_FIPS_info[v['FIPS']]['state'] = v['state']
                DF_FIPS_info[v['FIPS']]['county'] = v['Admin2']

        big_city_ids = set(DF[DF['Population_Density'] >= self.cutoff]['FIPS'])

        self._Calculate_Proximity(DF_FIPS_info,big_city_ids)

        DF['Closest_Big_City'] = DF['FIPS'].apply(lambda x : DF_FIPS_info[x]['Closest_City'])
        DF['Proximity'] = DF['FIPS'].apply(lambda x : DF_FIPS_info[x]['Proximity'])

        return DF
    
    def _Calculate_Proximity(self,df_fips,big_cities) :
    
        for fip in df_fips.keys() :
            lat,long = list(map(float,df_fips[fip]['lat_long'].split(',')))
            min_distance_city = 1e10
            for city in big_cities :
                lat_big_city,long_big_city = list(map(float,df_fips[city]['lat_long'].split(',')))
                dis = np.sqrt((lat_big_city - lat)**2 + (long_big_city - long)**2)
                if dis < min_distance_city :
                    min_distance_city = dis
                    min_city_name = df_fips[city]['county']
                    min_city_fip = city
            df_fips[fip]['Proximity'] = min_distance_city
            df_fips[fip]['Closest_City'] = min_city_name
            df_fips[fip]['Closest_City_FIPS'] = min_city_fip

    def _Normalize_Stats(self,DF) :

        DF['Positive_Cases_PopNormed'] = DF['Positive']/DF['population']
        DF['Deaths_PopNormed'] = DF['Deaths']/DF['population']

        DF['New_Positive_Cases'] = DF['Positive'] - DF.groupby('FIPS')['Positive'].shift().fillna(0) # lagged feat
        DF['New_Deaths'] = DF['Deaths'] - DF.groupby('FIPS')['Deaths'].shift().fillna(0) # lagged feat

        DF['New_Positive_Cases_PopNormed'] = DF['New_Positive_Cases']/DF['population'] #lagged, normalized feat
        DF['New_Deaths_PopNormed'] = DF['New_Deaths']/DF['population'] #lagged, normalized feat
        DF['Households_per_SqMile'] = DF['#_households'] / DF['Area (sqmi)']
        DF['Percent_in_Workforce'] = DF['number_in_workforce']/DF['population']

        cols_normalize = {'number_in_workforce':['drive_alone_to_work','carpool_to_work','public_transit_to_work','bus_trolley_to_work',
                  'walked_to_work','cab_other_means_of_transportation_to_work']}

        for k,v in cols_normalize.items() :
            for col in v :
                new_col = "%_"+col
                DF[new_col] = DF[col]/DF[k]

        DF['New_Positive_Cases_PopNormed_Lagged'] = DF.groupby('FIPS')['New_Positive_Cases_PopNormed'].shift(-1).fillna(0)

        DF = DF[~pd.isna(DF['Percent_in_Workforce'])]

        return DF
    
    def _Interpolate_Stats(self,DF,stat_fill_method) :

        for col in DF.columns :
            if np.sum(pd.isna(DF[col])) > 0 :
                print(col)
                try :
                    DF[col] = DF.groupby('FIPS')[col].apply(lambda group: group.interpolate(method=stat_fill_method,axis=0).ffill().bfill())
                except :
                    DF[col] = DF.groupby('state')[col].apply(lambda group: group.interpolate(method=stat_fill_method,axis=0).ffill().bfill())

            if np.sum(DF[col]==-666666666.0) > 0 :
                DF.loc[DF[col]==-666666666.0,col] = np.nan
                DF[col] = DF.groupby('state')[col].apply(lambda group: group.interpolate(method=stat_fill_method,axis=0,).ffill().bfill())

        return DF

    def write_out_json(self,df) :

        datatypes = df.dtypes.apply(lambda x: x.name).to_dict()
        datatypes['FIPS'] = 'object'

        with open(self.loc_write_dtypes, 'w') as f: #write out corrected datatypes dictionary in json format
            json.dump(datatypes, f)

        


