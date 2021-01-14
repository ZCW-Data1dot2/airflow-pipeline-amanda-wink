# Women's Ice Hockey Airflow Pipeline

### Uses Airflow to ingest, clean, and combine the statistics from the two professional women's ice hockey leagues (NWHL and CWHL) in their 2017 season.

<img width="193" alt="Screen Shot 2021-01-14 at 1 25 16 PM" src="https://user-images.githubusercontent.com/73121387/104637699-24967500-5673-11eb-8cb2-df3f64da9ce3.png">

###

<img width="523" alt="Screen Shot 2021-01-14 at 1 25 39 PM" src="https://user-images.githubusercontent.com/73121387/104637895-67f0e380-5673-11eb-9936-41d60f9bdd91.png">

###

The pipeline is composed of five Python operators. The first two operators create dataframes of the statistics for each league from two seperate excel files. Once the data is in a dataframe, it is cleaned using pandas. The cleaning process includes droping columns, adding columns, renaming columns, rearranging the order of columns, and filling nulls to create a consistent format between the two dataframes. Once both dataframes are cleaned, they are written to seperate csv files. The third operator creates a dataframe from each cleaned csv and concatenates them into one dataframe that is written to a new csv file. After the dataframes are combined, the fourth operator creates a dataframe from the combined csv and writes it to a MySQL table. The final operator queries the table and prints the top five players by goals scored from both the NWHL and CWHL. It also prints the top 20 goal scorers.

### 
Jupyter Notebooks were used originally to examine the data and test the cleaning process.

### Input Examples
#### NWHL
<img width="1182" alt="Screen Shot 2021-01-14 at 2 39 46 PM" src="https://user-images.githubusercontent.com/73121387/104640576-a76cff00-5676-11eb-8cbc-6c4bfe09b443.png">

#### CWHL
<img width="1012" alt="Screen Shot 2021-01-14 at 2 40 14 PM" src="https://user-images.githubusercontent.com/73121387/104640583-ab008600-5676-11eb-8643-c6d201686ed8.png">

### Final Output
<img width="276" alt="Screen Shot 2021-01-14 at 3 10 37 PM" src="https://user-images.githubusercontent.com/73121387/104646255-2f0a3c00-567e-11eb-916a-88cbb06b5a0a.png">

###

<img width="309" alt="Screen Shot 2021-01-14 at 3 11 10 PM" src="https://user-images.githubusercontent.com/73121387/104646262-30d3ff80-567e-11eb-9ccd-e3e7b78f02c7.png">

### Technologies/Languages
* Python
* Apache Airflow
* Pandas
* Jupyter Notebooks
