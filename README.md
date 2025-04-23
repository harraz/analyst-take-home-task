# Data Exercise

This data exercise represents an example of the type of data work we complete. We estimate that the exercise will take 2-3 hours to complete. Please use whatever statistical programming language, programming language, or data manipulation tool you are most comfortable with (SAS, R, SPSS, STATA, Python, SQL, etc).Please note, if you choose to alias tables in your code, we ask that you use descriptive aliases and not single letters.

This exercise will evaluate your ability to build a cohort of patients and calculate some metrics related to that cohort. You should have the following:

- [5 datasets](datasets) (all data you need is found within the provided datasets)
- [A data dictionary](data-dictionary.xlsx) defining each dataset and its fields

## Instructions

We encourage you to put the data ingest and export as part of a re-usable analysis script. Examples of different functions that can be used are found [here](data-import-setup). This makes it easier for us to re-run your code in our evaluations, and it's good coding practice!

### Part 1: Assemble the project cohort

The project goal is to identify patients seen for drug overdose, determine if they had an active opioid at the start of the encounter, and if they had any readmissions for drug overdose.

Your task is to assemble the study cohort by identifying encounters that meet the following criteria:

1. The patient’s visit is an encounter for drug overdose
2. The hospital encounter occurs after July 15, 1999
3. The patient’s age at time of encounter is between 18 and 35 (Patient is considered to be 35 until turning 36)

### Part 2: Create additional fields

With your drug overdose encounter, create the following indicators:

1. `DEATH_AT_VISIT_IND`: `1` if patient died during the drug overdose encounter, `0` if the patient died at a different time
2. `COUNT_CURRENT_MEDS`: Count of active medications at the start of the drug overdose encounter
3. `CURRENT_OPIOID_IND`: `1` if the patient had at least one active medication at the start of the overdose encounter that is on the Opioids List (provided below), 0 if not 
4. `READMISSION_90_DAY_IND`: `1` if the visit resulted in a subsequent drug overdose readmission within 90 days, 0 if not 
5. `READMISSION_30_DAY_IND`: `1` if the visit resulted in a subsequent drug overdose readmission within 30 days, 0 if not overdose encounter, `0` if not
6. `FIRST_READMISSION_DATE`: The date of the index visit's first readmission for drug overdose. Field should be left as `N/A` if no readmission for drug overdose within 90 days

### Part 3: Export the data to a `CSV` file

Export a dataset containing these required fields:

| Field name                | Field Description                                                                                                                  | Data Type        |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | ---------------- |
| `PATIENT_ID`              | Patient identifier                                                                                                                 | Character String |
| `ENCOUNTER_ID`            | Visit identifier                                                                                                                   | Character string |
| `HOSPITAL_ENCOUNTER_DATE` | Beginning of hospital encounter date                                                                                               | Date/time        |
| `AGE_AT_VISIT`            | Patient age at admission                                                                                                           | Num              |
| `DEATH_AT_VISIT_IND`      | Indicator if the patient died during the drug overdose encounter. Leave `N/A` if patient has not died,                             | 0 /1             |
| `COUNT_CURRENT_MEDS`      | Count of active medications at the start of the drug overdose encounter                          | Num              |
| `CURRENT_OPIOID_IND`      | if the patient had at least one active medication at the start of the overdose encounter that is on the Opioids List (provided below)     | 0/1              |
| `READMISSION_90_DAY_IND`  | Indicator if the visit resulted in a subsequent readmission within 90 days     | 0/1              |
| `READMISSION_30_DAY_IND`  | Indicator if the visit resulted in a subsequent readmission within 30 days     | 0/1              |
| `FIRST_READMISSION_DATE`  | Date of the first readmission for drug overdose within 90 days. Leave `N/A` if no readmissions for drug overdose within 90 days. | Date/time        |

## Opioids List:

- Hydromorphone 325Mg
- Fentanyl – 100 MCG
- Oxycodone-acetaminophen 100 Ml

## Submission Guidelines

Upon completion, please email the following to [DataRecruiting@email.chop.edu](mailto:DataRecruiting@email.chop.edu):

1. Data Exercise output dataset (`.csv`) (Please name the `.csv` file in the following format: "FIRSTNAME_LASTNAME.csv")
2. Data Exercise code (text file)

Good luck!

# Implementation Details

**Summary of the PySpark Notebook:**

The notebook implements a complete workflow to build a study cohort for patients seen for drug overdose and then calculate several key indicators related to those encounters. It begins by ingesting provided datasets (i.e patients, encounters, medications, allergies, and procedures) and uses a provided data dictionary to understand field definitions. The cohort is assembled by applying inclusion criteria: only encounters identified as drug overdose events that occurred after July 15, 1999, are included, and only patients aged between 18 and 35 at the time of the encounter are retained.

Following cohort assembly, the notebook creates additional derived fields using PySpark’s DataFrame API and window functions. For example, it calculates:

- **DEATH_AT_VISIT_IND:** A flag indicating whether the patient died during the overdose encounter versus at another time.
- **COUNT_CURRENT_MEDS:** The count of active medications at the start of the overdose encounter.
- **CURRENT_OPIOID_IND:** A binary indicator showing if at least one active medication is on a specified Opioids List.
- **READMISSION_90_DAY_IND and READMISSION_30_DAY_IND:** Indicators calculated using lead functions and date difference calculations that flag whether the encounter was followed by a subsequent drug overdose readmission within 90 days or 30 days, respectively.
- **FIRST_READMISSION_DATE:** This field is derived using window functions to extract the date of the first readmission within the 90-day window for each index encounter, with a value set to "N/A" if no such readmission exists.

Finally, the notebook includes steps to filter, inspect, and summarize these computed metrics. The complete workflow is designed with modularity and reusability in mind, culminating in the export of the final cohort data to a CSV file for downstream analysis or reporting. This approach demonstrates reproducible ETL and advanced data manipulation methods using PySpark, consistent with the best practices outlined in the data exercise instructions.

***More implementation details***
  
The [notebook](attemp1.ipynb) is a PySpark-based data processing workflow that loads clinical encounter data, performs data cleaning and joining, and then computes readmission indicators using window functions. In particular, it partitions the data by patient or encounter ID, orders records by encounter start or stop timestamps, and uses functions such as `lead()` and `datediff()` to calculate the time difference between successive encounters. Based on these differences, the notebook creates flags—for example, marking whether a readmission occurs within 30 or 90 days. It also demonstrates conditional column updates (for instance, replacing future encounter dates with "N/A" under certain conditions) and renames columns to standardize the output. Finally, the notebook includes steps to filter, display, and inspect intermediate results, effectively illustrating a complete ETL (Extract, Transform, Load) and analysis process for healthcare readmission data using PySpark.

