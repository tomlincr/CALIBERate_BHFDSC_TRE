# CALIBERate_BHFDSC_TRE  
This repository contains a series of databricks notebooks (Jupyter-like combining `python` & `sql` with `markdown`) used within the [BHFDSC CVD-COVID-UK Trusted Research Environment for England](https://web.www.healthdatagateway.org/dataset/7e5f0247-f033-4f98-aed3-3d7422b9dc6d) to map patient-level data from both Primary Care ([GPES Data for Pandemic Planning and Research (COVID-19)](https://web.www.healthdatagateway.org/dataset/696cfc9f-090d-4328-94ac-140760a77c73)) and Secondary Care ([Hospital Episode Statistics Admitted Patient Care](https://web.www.healthdatagateway.org/dataset/2b6409db-a669-4bef-9fd5-39c2b6f8d5e9)) to the [CALIBER Phenotypes](https://portal.caliberresearch.org/) described in:  
>Kuan V., Denaxas S., Gonzalez-Izquierdo A. et al. _A chronological map of 308 physical and mental health conditions from 4 million individuals in the National Health Service_ published in the Lancet Digital Health - DOI <a href="https://www.thelancet.com/journals/landig/article/PIIS2589-7500(19)30012-3/fulltext">10.1016/S2589-7500(19)30012-3</a>  

and available at [https://github.com/spiros/chronological-map-phenotypes](https://github.com/spiros/chronological-map-phenotypes).  
  
This code is shared in the hope that it may provide a useful approach to other users working within the same TRE but also to those working with similar data in different research environments.  

Users working within the TRE under the `dars_nic_391419_j3w9t` access agreement may wish to make use of the output tables, particularly `dars_nic_391419_j3w9t_collab.ccu013_caliber_patients` already generated to reduce the need for further computation.  

<br>
<br>


# [`01_build_caliber_codelist.py`](/01_build_caliber_codelist.py)

### Inputs:
* ALL `caliber_...` tables in `bhf_cvd_covid_uk_byod` database 

### Overview:    
1. Imports the CALIBER codelist dictionary from *Kuan V., Denaxas S., Gonzalez-Izquierdo A. et al. A chronological map of 308 physical and mental health conditions from 4 million individuals in the National Health Service published in the Lancet Digital Health - DOI 10.1016/S2589-7500(19)30012-3* at https://github.com/spiros/chronological-map-phenotypes  
    * NB owing to lack of external connectivity within the TRE, this is imported by hardcoding  
2. Applies a series of 'regex' transformations to map the dictionary to the format of codelists stored within the TRE `bhf_cvd_covid_uk_byod` database  
3. Uses the transformed dictionary to loop through all relevant codelists and compile a **master codelist** of: `phenotype`, `code` & `terminology`

### Outputs:
* `ccu013_caliber_dictionary`
* `ccu013_caliber_codelist_master`  

<br>
<br>

# [`02_build_caliber_skinny.py`](/02_build_caliber_skinny.py)

### Inputs:  
* Codelist:   
  * `ccu013_caliber_codelist_master`
* Datasets: (NB working off the raw datasets, not freezes)  
  * GDPPR: `dars_nic_391419_j3w9t.gdppr_dars_nic_391419_j3w9t`
  * HES APC: `dars_nic_391419_j3w9t_collab.hes_apc_all_years`

### Overview:  
1. For each terminology in `ccu013_caliber_codelist_master`
2. Join data source with codelist on `code` to get `phenotype`:
  * 1. `terminology = ICD` -> HES APC DIAG
  * 2. `terminology = OPCS` -> HES APC OP
  * 3. `terminology = SNOMED` -> GDPPR
3. Unite & agreggate to produce a 'skinny table' of patients, `phenotype` and `date`

### Outputs:
* `ccu013_caliber_patients` = 'skinny' table of each mention of phenotype per pt
* Intermediate outputs:  
  * `ccu013_caliber_tmp_pts_gdppr`  
  * `ccu013_caliber_tmp_data_apc_icd`  
  * `ccu013_caliber_tmp_data_apc_opcs` 

<br>
<br>

# [`03_categories.py`](/03_categories.py)  

### Inputs:
* `ccu013_caliber_patients`

### Overview:  
This maps 306 of the CALIBER phenotypes by *Kuan et al.* to the following 16 categories:

* Diseases of the Circulatory System  
* Diseases of the Digestive System  
* Skin conditions  
* Diseases of the Genitourinary system  
* Haematological/Immunological conditions  
* Mental Health Disorders  
* Diseases of the Respiratory System  
* Musculoskeletal conditions  
* Diseases of the Eye  
* Neurological conditions  
* Infectious Diseases  
* Benign Neoplasm/CIN  
* Perinatal conditions  
* Diseases of the Endocrine System  
* Diseases of the Ear  
* Cancers  

These 16 categories are taken from *Supplementary Table S1. Medical Conditions with the abbreviated terms used in this article and their disease categories* from the paper:  
>Kuan V., Denaxas S., Gonzalez-Izquierdo A. et al. _A chronological map of 308 physical and mental health conditions from 4 million individuals in the National Health Service_ published in the Lancet Digital Health - DOI <a href="https://www.thelancet.com/journals/landig/article/PIIS2589-7500(19)30012-3/fulltext">10.1016/S2589-7500(19)30012-3</a>  

Finally this notebook applies `regex` transformations consistent with those used in notebook `01_build_caliber_codelist` to produce `ccu013_caliber_codelist_master`

### Outputs:
* `ccu013_caliber_category_mapping`
* `ccu013_caliber_patients_category_code`