import etl
import updateTables

"""
To change where the files are stored, just change the paths to the correct filepath on your machine.
"""
def main():
  midPath=r'C:\Users\$$$\Western Washington University\Colton Ferriole Rice - Interactive Factbook\Test'
  endPath=r'C:\Users\$$$\Western Washington University\Colton Ferriole Rice - Interactive Factbook\Tables' 
  years=['2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022','2023']
  try:
    etl.pullData(years=years,midPath=midPath)
    Enrollment,rates=etl.cleanData(years=years,midPath=midPath, endPath=endPath)
    etl.dfsToParquet(ENROLLMENT=Enrollment,RATES=rates,midPath=midPath, endPath=endPath)
    print("Success!")
  except:
    print("Something went wrong")

main()