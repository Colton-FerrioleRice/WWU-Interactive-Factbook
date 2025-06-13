"""
This is a utility function that will update the enrollment and rates table with new years, as long as both tables
exist with old data in them. This is done so that updating the data only has to download the new data, not all 
of it.  
"""
def updateTables(midPath,endPath,years):
   prevEnrollment=pd.read_parquet(endPath+r'\ENROLLMENT_WIDE.parquet')
   prevRates=pd.read_parquet(endPath+r'\RATES_WIDE.parquet')
   pullData(years,midPath=midPath)
   newEnrollment,newRates=cleanData(years,midPath,endPath)
   ENROLLMENT=pd.concat([prevEnrollment, newEnrollment])
   RATES=pd.concat([prevRates, newRates])
   return ENROLLMENT, RATES