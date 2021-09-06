# Master Thesis on ICU data from the MIMIC-III Dataset
By I Vivek Kai-wen and Kevan Amini

This project makes use of ICU data from the MIMIC-III Dataset to build 2 models. 

First, we built an end-of-day mortality prediction model for ICU patients. Mortality is notoriously difficult to predict for ICU patients due to the unstable health situation that they are in. Nonetheless, using some engineered features such as comorbidities and severity scores, we achieved high levels of precision and sensitivity. 

Next, we built an agent-based model that enables better planning of resources by the hospital management. This complex agent-based model was built on many smaller submodels, including a model to generate new patients with realistic health data and the previous mortality prediction model. 

The slides used for the presentation of our findings is entitled `MA_Final_Presentation_V01.pptx.pdf`.

Following these steps, provided that the required access rights are given, you will be able to reproduce our results:

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
