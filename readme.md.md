## Overview

This repo scrapes data from [IPEDS (Integrated Postsecondary Education Data System)](https://nces.ed.gov/ipeds/use-the-data), processes it, and structures it into clean formats ready for visualization in Power BI.

A matching Power BI dashboard allows for easy interaction with the data, enabling dynamic filtering, extraction, and reporting.  

> See [📊 Dashboard Section](#custom-powerbi-dashboard) for screenshots and guidance on use.

---

## Data Acquisition

### Script: `ipeds_pipeline.py`

#### How to run:

1. **Decide on a landing folder** for the output data  
2. Create a virtual environment and activate it:
3. Run the pipeline script:
   ```bash
   python ipeds_pipeline.py --output /your/landing/folder
   ```

---

## Data Processing

The script:
- Cleans raw IPEDS data
- Standardizes and renames variables
- Merges datasets using keys `unitid` and `year`
- Outputs in **two formats**:
  - **Wide format**: 1 row per institution-year
  - **Tall format**: stacked form-data (good for pivoting in Power BI)

---

## Custom PowerBI Dashboard

### Opening the Dashboard

1. Open **Power BI Desktop**
2. Load the `.pbix` file 
3. Insert the Data model
4. If prompted, update the **data source folder** to match your landing folder

![Data model](images/Data%20Model.png)
### Connecting to Data

1. Click **Transform Data**
2. Replace data source paths with your new `merged_wide.csv` and `merged_tall.csv`
3. Click **Close & Apply**

---

## Using Power BI Features

### 1. Using Slicers (Dropdown Filters)

Slicers are interactive dropdowns for filtering visuals. As you select more filters, options may disappear as there is no data in that cross section.

#### Steps:

1. Click a slicer 
2. Use the dropdown to select values (e.g., Year, State)
3. Multi-select
4. All graphs will update accordingly

 *Insert screenshot of slicer use*

---

### 2. Exporting Data

You can export raw data from any chart or table:

#### Steps:

1. **Right-click** on any chart or table
2. Select **“Export data”**
3. Choose `.csv` or `.xlsx`
4. Save the exported file

 ![Export Data](images/Export%20Data%202.png)

---

## Adding New Variables (Scraped Forms)

You can include variables from new forms by merging with the existing data.

#### Steps:

1. Add the new form CSV to your directory
2. Modify `merge_survey_forms.ipynb`:
   ```python
   df_newform = pd.read_csv('newform.csv')
   df_newform_cleaned = clean_variables(df_newform)
   merged = pd.merge(merged, df_newform_cleaned, on=['unitid', 'year'], how='left')
   ```

---

## Updating Tables
Use updateTables.py to grab the new years


To add variables to the institutions table:

1. Edit `institutions() in etl.py
2. Add a new row or entry:
   ```python
 new_mapping = {
	 'pref_name' : 'column_name'
 }
   ```
3. Make sure the `unitid` exists in your form data

---

## Tall vs. Wide Format Explained

| Format | Description | Use Case |
|--------|-------------|----------|
| **Wide** | One row = one institution-year | Good for summary reporting |
| **Tall** | One row = one variable observation | Ideal for Power BI visualizations |

---

## Setup From Scratch (End-to-End)

1. Clone this repo
2. Set up your Python environment
3. adjust the file path in main.py
4. Run main.py
5. Open Power BI
6. Link the data source
7. Start exploring the dashboard

---

## Future Usage Notes

- Check for IPEDS file names and ZIP structures changes
- Variable names may shift year-to-year or new vars added
- Scraped forms may need periodic updates or manual inspection

---

## Future Work / TODOs

- Automate Power BI dashboard refresh
- Track schema/versioning changes in IPEDS
- Add scraping for external education data sources
- Integrate an AI Agent for NLP-based data querying

---

## Contact

Questions or suggestions?

Email:
Maintainer: 
