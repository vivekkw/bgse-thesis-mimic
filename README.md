# bgse-thesis-mimic
Master Thesis on ICU data from the MIMIC-III Dataset

Following these steps, provided that the required access rights are given, you should be able to reproduce our results:

1) Run `Query_from_Cloud.R` file 

    This will output following 5 csv files: 
  * `procedures.csv`
  * `caregivers_count_df.csv`
  * `severity_scores_df.csv`
  * `all_diagnoses.csv`
  * `vitals_df.csv`
  
2) Next, run the `create_dfs.py` script in the terminal to create additionally these two csv files:
* `df_snapshots.csv`
* `main_df2.csv`

3) Set up the environment with all packages needed for running the subsequent notebooks in step 4) and 5) by running `pip install -r requirements.txt`

4) Run the `Models_Mortality_Prediction_Final.ipynb` notebook to see the results for the **single-task prediction model** (mortality prediction)

    *Please note: This step requires that the files generated in step 1) and 2) are in the same directory*

5) Run the `main_code.ipynb` notebook to see the results for the **multi-task prediction model** (ABM)
   
   *Please note: Towards the end of the notebook is the code for the ABM BatchRunner. Running those bits of the code is computationally expensive. Expected runtime for creating the ABM output files will be approx. (3x16hours)*
