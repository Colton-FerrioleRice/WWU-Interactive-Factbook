import re
import os
import requests
import zipfile
import pandas as pd
"""
This method will scrape ipeds for the raw data files that we need, and store them in a google drive folder
years --> a list of years that you want to scrape

This method loops through the years given, and for each year, loops through each iped form we need. for each of these forms, download the
zip, and then extract it to the given folder.

To change where to store the raw files, change the download_folder path to your needed path
To update for new years, just input the latest year by itself

To Do: standardize the naming of the downloaded files by lowercasing. Ipeds is not consistent about this in the earlier years
there might be a way to do this without also storing the unzipped file
Skip the download if the file already exists in the download folder
"""
def pullData(years, midPath):
  print("Pulling ipeds data...")
  count=0
  for year in years:
    pastYcurrent=str(int(year)-1)[-2:]+year[-2:]
    forms=['EFFY'+year,'EF'+year+'A','EF'+year+'B','EF'+year+'C','EF'+year+'D','HD'+year,'HD'+year+'_DICT','SFA'+pastYcurrent,'GR'+year]
    for form in forms:
      count+=1
      print("Downloading and extracting: "+form +" | "+str(round(100*count/(len(years)*len(forms))))+'%')
      zip_file = form+'.zip'
      root = r'https://nces.ed.gov/ipeds/datacenter/data/'
      download_folder = midPath
      full_url = root + zip_file
      r = requests.get(full_url)
      dl_path = os.path.join(download_folder, zip_file)
      with open(dl_path, 'wb') as z_file:
          z_file.write(r.content)

      extract_dir = os.path.splitext(os.path.basename(zip_file))[0]
      try:
        z = zipfile.ZipFile(dl_path)
        z.extractall(os.path.join(download_folder, extract_dir))
      except zipfile.BadZipfile:
        pass
  print('Data pull complete!')

  """
This method will take all the forms for each year, and combine them into to large tables, ENROLLMENT and RATES
years --> A list of years we want to grab ipeds data from
shape --> wide, tall, or both, will choose the output format of the tables. Default is wide

For each year given, pulls in each form that needed for that year and creates a dataframe. Then filters out the unwanted columns and renames
columns and variables for better readability. Next, the method does a lot of formatting stuff to get the data we need out of ipeds format into
the desired format. Finally, writes the final dataframes to the google drive folder.

This will need to be run over all years every time the data is updated, even if only 1 year of data is new

To do: Clean up the formatting stuff. Im sure the code is doing stuff that is not used in the final tables, just because i forgot to update it
as i worked. Code is very messy and hard to understand near the bottom so i need to clean that up too.
Need to clean up long paths that are repeated
"""



def cleanData(years,midPath, endPath):
  print("Cleaning data...")
  #initialize the main tables
  ENROLLMENT=pd.DataFrame()
  RACEETHNICITY=pd.DataFrame()
  RATES=pd.DataFrame()
  count=0
  #This will loop through each year specified, and do the needed transformations and cleanup
  for year in years:
    count+=1
    print("Cleaning data from: "+year+" | "+str(round(100*count/len(year)))+'%')
    pastYcurrent=str(int(year)-1)[-2:]+year[-2:]
    #Total Enrollment Info
    EFFY2018 = pd.read_csv(midPath+r'/EFFY'+year+r'/effy'+year+'.csv', header = 0)
    #Race, Ethnicity, Gender of students
    EF2018A = pd.read_csv(midPath+r'/EF'+year+r'A/ef'+year+'a.csv', header = 0)
    #Enrollment by Age group of students
    EF2018B = pd.read_csv(midPath+r'/EF'+year+r'B/ef'+year+'b.csv', header = 0)
    #Enrollment by resident state
    EF2018C = pd.read_csv(midPath+r'/EF'+year+r'C/ef'+year+'c.csv', header = 0)
    #Retention Rates
    EF2018D= pd.read_csv(midPath+r'/EF'+year+r'D/ef'+year+'d.csv', header = 0)
    #Graduation Rates
    GR2018 = pd.read_csv(midPath+r'/GR'+year+r'/gr'+year+'.csv', header = 0)
    #Student Financial Aid
    SFA = pd.read_csv(midPath+r'/SFA'+pastYcurrent+r'/sfa'+pastYcurrent+'.csv', header = 0)


    #This section of code is just taking in every raw form, filtering out variables we want, and renaming some variables for readability
    EFFY2018=EFFY2018.filter(['UNITID','EFFYLEV','EFYTOTLT','EFYTOTLM','EFYTOTLW'])
    EFFY2018.rename(columns = {'EFFYLEV':'Student Level','EFYTOTLT':'Total Students','EFYTOTLM':'Total Male Students','EFYTOTLW':'Total Female Students'}, inplace = True)
    EFFY2018=EFFY2018.replace({'Student Level':{1:'All Levels',2:'Undergraduate Students',4:'Graduate Students',-2:'Not Applicable'}})
    EF2018A=EF2018A.filter(['UNITID','EFALEVEL','LINE','LSTUDY','SECTION','EFUNKNT','EFAIANT','EFASIAT','EFBKAAT','EFHISPT','EFNHPIT','EFWHITT','EF2MORT'])
    EF2018A.rename(columns = {'EFALEVEL':'Level and Degree Seeking Status','LSTUDY':'Level of Student','SECTION':'Full time/Part time/All students', 'EFUNKNM':'Race/Ethnicity Unknown Male','EFUNKNW':'Race/Ethnicity Unknown Female','EFUNKNT':'Race/Ethnicity Unknown','EFAIANT':'American Indian or Alaska Native','EFASIAT':'Asian','EFBKAAT':'Black or African American','EFHISPT':'Hispanic','EFNHPIT':'Native Hawaiian or Other Pacific Islander','EFWHITT':'White','EF2MORT':'Two or more races'}, inplace = True)

    EF2018B=EF2018B.filter(['UNITID','EFBAGE','LSTUDY','EFAGE01','EFAGE02','EFAGE03','EFAGE04','EFAGE05','EFAGE06','EFAGE07','EFAGE08','EFAGE09'])
    EF2018B.rename(columns={'EFBAGE':'Age Catagory','LSTUDY':'Level of Student','EFAGE01':'Full Time Men','EFAGE02':'Full Time Women','EFAGE03':'Part Time Men','EFAGE04':'Part Time Women','EFAGE05':'Full Time Total','EFAGE06':'Part Time Total','EFAGE07':'Total Men','EFAGE08':'Total Women','EFAGE09':'Grand Total'},inplace=True)
    EF2018B=EF2018B.replace({'Age Catagory':{1:'All age categories total',2:'Age under 25 total',3:'Age under 18',4:'Age 18-19',5:'Age 20-21',6:'Age 22-24',7:'Age 25 and over total',8:'Age 25-29',9:'Age 30-34',10:'Age 35-39',11:'Age 40-49',12:'Age 50-64',13:'Age 65 and over',14:'Age unknown'},'Level of Student':{1:'All Students total',2:'Undergraduate',5:'Graduate'}})

    EF2018C=EF2018C.filter(['UNITID','EFCSTATE','EFRES01','EFRES02'])
    EF2018C.rename(columns={'EFCSTATE':'State','EFRES01':'First-time degree/certificate-seeking undergraduate students','EFRES02':'First-time degree/certificate-seeking undergraduate students who graduated from high school in the past 12 months'},inplace=True)

    EF2018D=EF2018D.filter(['UNITID','RET_PCF','RET_PCP'])
    EF2018D.rename(columns={'RET_PCF':'Full-time retention rate','RET_PCP':'Part-time Retention Rate'}, inplace =True)

    GR2018=GR2018.filter(['UNITID','GRTYPE','GRTOTLT'])
    GR2018.rename(columns={'GRTYPE':'Cohort Data','GRTOTLT':'Grand Total'},inplace=True)

    SFA= SFA.filter(['UNITID','UPGRNTN', 'UPGRNTA'])
    SFA.rename(columns = {'UPGRNTN':'Number of Undergrad granted pell','UPGRNTA':'Average Award for Undergrad'}, inplace = True)

    #Type Conversions
    EFFY2018['UNITID']=EFFY2018['UNITID'].astype(str)
    EF2018A['UNITID']=EF2018A['UNITID'].astype(str)
    EF2018B['UNITID']=EF2018B['UNITID'].astype(str)
    EF2018C['UNITID']=EF2018C['UNITID'].astype(str)
    EF2018D['UNITID']=EF2018D['UNITID'].astype(str)
    SFA['UNITID']=SFA['UNITID'].astype(str)
    GR2018['UNITID']=GR2018['UNITID'].astype(str)

    #Setting up the rates data for the current year
    Rates=pd.DataFrame(columns=['UNITID','Year'])
    Rates['UNITID']=EF2018D.filter(['UNITID'])
    Rates['Year']=year
    Rates=pd.merge(Rates,EF2018D,on='UNITID')

    #setting up the race and ethnicity data for the current year
    RaceEthnicity=pd.DataFrame(columns=['UNITID','Year'])
    RaceEthnicity['UNITID']=EFFY2018.filter(['UNITID'])
    RaceEthnicity['Year']=year
    RaceEthnicity=pd.merge(RaceEthnicity,EF2018A[EF2018A['Level and Degree Seeking Status']==1],on='UNITID').drop(columns=['Level and Degree Seeking Status','Level of Student','Full time/Part time/All students','LINE'])
    RaceEthnicity=RaceEthnicity.drop_duplicates(subset='UNITID')

    #setting up the enrollment data for the current year and reformatting ipeds data into a more useful format
    Enrollment=pd.DataFrame(columns=['UNITID','Year'])
    Enrollment['UNITID']=EFFY2018.filter(['UNITID'])
    Enrollment['Year']=year
    Enrollment=pd.merge(Enrollment,EFFY2018[EFFY2018['Student Level']=='All Levels'],on='UNITID')
    Enrollment=pd.merge(Enrollment,EFFY2018[EFFY2018['Student Level']=='Graduate Students'].drop(columns=['Student Level','Total Male Students','Total Female Students']).rename(columns={'Total Students':'Total_Graduate'}),on='UNITID')
    Enrollment=pd.merge(Enrollment,EFFY2018[EFFY2018['Student Level']=='Undergraduate Students'].drop(columns=['Student Level','Total Male Students','Total Female Students']).rename(columns={'Total Students':'Total_Undergraduate'}),on='UNITID')
    ALLLEVELS=EF2018B[EF2018B['Level of Student']=='All Students total']
    TIME=ALLLEVELS[ALLLEVELS['Age Catagory']=='All age categories total'].drop(columns=['Age Catagory','Level of Student','Total Men','Total Women'])
    Enrollment=pd.merge(Enrollment, SFA, on='UNITID')
    Enrollment=pd.merge(Enrollment,GR2018, on='UNITID')
    Enrollment=pd.merge(Enrollment,TIME,on='UNITID')

    #getting the age catagory data and inputting into the enrollment table
    ALLLEVELS=EF2018B[EF2018B['Level of Student']=='All Students total']
    UNDER18=ALLLEVELS[ALLLEVELS['Age Catagory']=='Age under 18'].drop(columns=['Age Catagory','Level of Student','Full Time Men','Full Time Women','Part Time Men','Part Time Women','Full Time Total','Part Time Total'])
    UNDER18['Under_18']=UNDER18['Total Men']+UNDER18['Total Women']
    UNDER18=UNDER18.drop(columns=['Total Men','Total Women'])
    Enrollment=pd.merge(Enrollment,UNDER18,on='UNITID')

    ALLLEVELS=EF2018B[EF2018B['Level of Student']=='All Students total']
    AGE1819=ALLLEVELS[ALLLEVELS['Age Catagory']=='Age 18-19'].drop(columns=['Age Catagory','Level of Student','Full Time Men','Full Time Women','Part Time Men','Part Time Women','Full Time Total','Part Time Total'])
    AGE1819['18_to_19']=AGE1819['Total Men']+AGE1819['Total Women']
    AGE1819=AGE1819.drop(columns=['Total Men','Total Women'])
    Enrollment=pd.merge(Enrollment,AGE1819,on='UNITID')

    ALLLEVELS=EF2018B[EF2018B['Level of Student']=='All Students total']
    AGE1819=ALLLEVELS[ALLLEVELS['Age Catagory']=='Age 20-21'].drop(columns=['Age Catagory','Level of Student','Full Time Men','Full Time Women','Part Time Men','Part Time Women','Full Time Total','Part Time Total'])
    AGE1819['20_to_21']=AGE1819['Total Men']+AGE1819['Total Women']
    AGE1819=AGE1819.drop(columns=['Total Men','Total Women'])
    Enrollment=pd.merge(Enrollment,AGE1819,on='UNITID')

    ALLLEVELS=EF2018B[EF2018B['Level of Student']=='All Students total']
    AGE1819=ALLLEVELS[ALLLEVELS['Age Catagory']=='Age 22-24'].drop(columns=['Age Catagory','Level of Student','Full Time Men','Full Time Women','Part Time Men','Part Time Women','Full Time Total','Part Time Total'])
    AGE1819['22_to_24']=AGE1819['Total Men']+AGE1819['Total Women']
    AGE1819=AGE1819.drop(columns=['Total Men','Total Women'])
    Enrollment=pd.merge(Enrollment,AGE1819,on='UNITID')

    ALLLEVELS=EF2018B[EF2018B['Level of Student']=='All Students total']
    AGE1819=ALLLEVELS[ALLLEVELS['Age Catagory']=='Age 25-29'].drop(columns=['Age Catagory','Level of Student','Full Time Men','Full Time Women','Part Time Men','Part Time Women','Full Time Total','Part Time Total'])
    AGE1819['25_to_29']=AGE1819['Total Men']+AGE1819['Total Women']
    AGE1819=AGE1819.drop(columns=['Total Men','Total Women'])
    Enrollment=pd.merge(Enrollment,AGE1819,on='UNITID')

    ALLLEVELS=EF2018B[EF2018B['Level of Student']=='All Students total']
    AGE1819=ALLLEVELS[ALLLEVELS['Age Catagory']=='Age 25 and over total'].drop(columns=['Age Catagory','Level of Student','Full Time Men','Full Time Women','Part Time Men','Part Time Women','Full Time Total','Part Time Total'])
    AGE1819['Over_30']=AGE1819['Total Men']+AGE1819['Total Women']
    AGE1819=AGE1819.drop(columns=['Total Men','Total Women'])
    Enrollment=pd.merge(Enrollment,AGE1819,on='UNITID')
    Enrollment['Over_30']=Enrollment['Over_30']-Enrollment['25_to_29']

    #Doing some final reformatting and concatting the current years data with the data from all previous years into the main tables
    Enrollment = Enrollment.drop_duplicates(subset='UNITID')
    Enrollment=Enrollment.rename(columns={'Total Students':'Total_Students','Total Male Students':'Total_Male_Students','Total Female Students':'Total_Female_Students','Full Time Men':'Full_Time_Men','Full Time Women':'Full_Time_Womwn','Part Time Men':'Part_Time_Men','Part Time Women':'Part_Time_Women','Full Time Total':'Full_Time_Total','Part Time Total':'Part_Time_Total'})
    Enrollment=Enrollment.drop(columns=['Student Level'])
    ENROLLMENT=pd.concat([ENROLLMENT,Enrollment],ignore_index=True,axis=0)
    RACEETHNICITY=pd.concat([RACEETHNICITY,RaceEthnicity],ignore_index=True,axis=0)
    RATES=pd.concat([RATES,Rates],ignore_index=True,axis=0)

  #Merging ethnicity and enrollment data into a single table
  ENROLLMENT=pd.merge(ENROLLMENT,RACEETHNICITY,on=['UNITID','Year'])
  print("Data transformation complete!")
  return ENROLLMENT,RATES

"""
This is a utility function which converts the enrollment, rates, and institutions tables to parquet files
"""
def dfsToParquet(ENROLLMENT,RATES, midPath,endPath, shape='both'):
  print("Converting tables to parquet files...")
  INSTITUTIONS=institutions(midPath)
  INSTITUTIONS.to_parquet(endPath+'/INSTITUTIONS.parquet')
   #basically this is a big switch statement that chooses where and in what format to store each table
  if(shape=='tall'):
    ENROLLMENT=pd.melt(ENROLLMENT,id_vars=['UNITID','Year'],var_name='Variable',value_name='Value')
    ENROLLMENT.to_parquet(endPath+'/ENROLLMENT_TALL.parquet')
    RATES=pd.melt(RATES,id_vars=['UNITID','Year'],var_name='Variable',value_name='Value')
    RATES.to_parquet(endPath+'RATES_TALL.parquet')
  elif(shape=='wide'):
    ENROLLMENT.to_parquet(endPath+'/ENROLLMENT_WIDE.parquet')
    RATES.to_parquet(endPath+'/RATES_WIDE.parquet')
  elif(shape=='both'):
    ENROLLMENT.to_parquet(endPath+'/ENROLLMENT_WIDE.parquet')
    RATES.to_parquet(endPath+'/RATES_WIDE.parquet')
    ENROLLMENT=pd.melt(ENROLLMENT,id_vars=['UNITID','Year'],var_name='Variable',value_name='Value')
    ENROLLMENT.to_parquet(endPath+'/ENROLLMENT_TALL.parquet')
    RATES=pd.melt(RATES,id_vars=['UNITID','Year'],var_name='Variable',value_name='Value')
    RATES.to_parquet(endPath+'/RATES_TALL.parquet')
  else:
    print('invalid shape')
  print('Data write complete!')

  # --- Utility Functions ---
def build_mapping(dfd, varname: str) -> dict:
    """
    Builds a dictionary from a DataFrame for a given variable name.
    Skips non-integer code values.
    """
    filtered = dfd[dfd['varname'].str.strip().str.lower() == varname.lower()]
    mapping = {}
    for _, row in filtered.iterrows():
        try:
            key = int(row['codevalue'])
            value = str(row['valuelabel']).strip()
            mapping[key] = value
        except ValueError:
            continue  # Skip non-integer keys
    return mapping

def institutions(midPath):
  # --- Hardcoded Mappings ---
  """
  Insert OCR and fuzzy match scraper here."""
  peer_mapping_2020 = { # 7/7
      'California Polytechnic State University-San Luis Obispo': 'Aspirational Peer',
      'Georgia College & State University': 'Aspirational Peer',
      'Stockton University': 'Aspirational Peer',
      'Appalachian State University': 'Aspirational Peer',
      'University of North Carolina Wilmington': 'Aspirational Peer',
      'College of Charleston': 'Aspirational Peer',
      'James Madison University': 'Aspirational Peer'
  }

  challenge_mapping_2008 = { # 8/11
      'California Polytechnic State University-San Luis Obispo': 'Global_Challenge_States_Peer',
      'California State University‐Chico': 'Global_Challenge_States_Peer',
      'California State Polytechnic University-Pomona': 'Global_Challenge_States_Peer',
      'Southern Connecticut State University': 'Global_Challenge_States_Peer',
      'Central Connecticut State University': 'Global_Challenge_States_Peer',
      'Towson University': 'Global_Challenge_States_Peer',
      'Montclair State University': 'Global_Challenge_States_Peer',
      'William Paterson University of New Jersey': 'Global_Challenge_States_Peer',
      'Rowan University': 'Global_Challenge_States_Peer',
      'James Madison University': 'Global_Challenge_States_Peer',
      'Radford University': 'Global_Challenge_States_Peer'
  }

  board_approved_peer_list_2004 = { #15/24
      'California Polytechnic State University-San Luis Obispo': 'Board_Approved_Peer',
      'California State University‐Chico': 'Board_Approved_Peer',
      'California State Polytechnic University-Humboldt': 'Board_Approved_Peer',
      'Sonoma State University': 'Board_Approved_Peer',
      'University of Colorado Colorado Springs': 'Board_Approved_Peer',
      'University of Northern Iowa': 'Board_Approved_Peer',
      'Eastern Illinois University': 'Board_Approved_Peer',
      'Washburn University': 'Board_Approved_Peer',
      'Murray State University': 'Board_Approved_Peer',
      'Salisbury University': 'Board_Approved_Peer',
      'Towson University': 'Board_Approved_Peer',
      'Truman State University': 'Board_Approved_Peer',
      'Appalachian State University': 'Board_Approved_Peer',
      'University of North Carolina at Charlotte': 'Board_Approved_Peer',
      'University of North Carolina Wilmington': 'Board_Approved_Peer',
      'Rowan University': 'Board_Approved_Peer',
      'The College of New Jersey': 'Board_Approved_Peer',
      'SUNY College at Geneseo': 'Board_Approved_Peer',
      'Millersville University of Pennsylvania': 'Board_Approved_Peer',
      'College of Charleston': 'Board_Approved_Peer',
      'Winthrop University': 'Board_Approved_Peer',
      'James Madison University': 'Board_Approved_Peer',
      'University of Wisconsin-Eau Claire': 'Board_Approved_Peer',
      'University of Wisconsin‐La Crosse': 'Board_Approved_Peer',
      'University of Wisconsin‐Stevens Point': 'Board_Approved_Peer'
  }

  # --- Data Loading ---
  df = pd.read_csv(midPath+'/HD2023/hd2023.csv', encoding='latin-1')
  dfd = pd.read_excel(midPath+'/HD2023_DICT/hd2023.xlsx', sheet_name="Frequencies")

  # --- Data Preparation ---
  """
  This is where you would add new columns or modify existing ones.
  """
  rename_mapping = {
      'UNITID': 'UNITID',
      'INSTNM': 'UNIVERSITY_NAME',
      'STABBR': 'STATE',
      'SECTOR': 'INSTITUTION_TYPE',
      'ICLEVEL': 'PROGRAM_LENGTH',
      'C21BASIC': 'CARNEGIE_CLASSIFICATION',
      'LONGITUD': 'LONGITUDE',
      'LATITUDE': 'LATITUDE'
  }

  # Filter to keep only the renamed columns
  df = df.rename(columns=rename_mapping)
  df = df[list(rename_mapping.values())]

  df['PEER_GROUP'] = None

  # Build mappings dynamically from the Excel file
  program_length_mapping = build_mapping(dfd, 'ICLEVEL')
  carnegie_mapping = build_mapping(dfd, 'C21BASIC')
  sector_mapping = build_mapping(dfd, 'SECTOR')
  sector_mapping = {key: value.split(',')[0] for key, value in sector_mapping.items()}

  # Apply mappings
  df['PROGRAM_LENGTH'] = df['PROGRAM_LENGTH'].map(program_length_mapping)
  df['INSTITUTION_TYPE'] = df['INSTITUTION_TYPE'].map(sector_mapping)
  df['CARNEGIE_CLASSIFICATION'] = df['CARNEGIE_CLASSIFICATION'].map(carnegie_mapping)

  # Print mappings with appearance frequency
  print("PROGRAM_LENGTH Mapping with Counts:")
  print(df['PROGRAM_LENGTH'].value_counts())

  print("\nINSTITUTION_TYPE Mapping with Counts:")
  print(df['INSTITUTION_TYPE'].value_counts())

  print("\nCARNEGIE_CLASSIFICATION Mapping with Counts:")
  print(df['CARNEGIE_CLASSIFICATION'].value_counts())

  # Apply peer group mappings
  df['PEER_GROUP'] = df['UNIVERSITY_NAME'].map(peer_mapping_2020)
  df['PEER_GROUP'] = df['PEER_GROUP'].fillna(df['UNIVERSITY_NAME'].map(challenge_mapping_2008))
  df['PEER_GROUP'] = df['PEER_GROUP'].fillna(df['UNIVERSITY_NAME'].map(board_approved_peer_list_2004))

  # --- Output ---
  return df
