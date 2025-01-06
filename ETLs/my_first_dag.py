# Import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow.models import DAG
# Operators; you need this to write tasks!
from airflow.operators.python import PythonOperator

# This makes scheduling easy
from airflow.utils.dates import days_ago

# Define the path for the input and output files
input_file = '/etc/passwd'
extracted_file = 'extracted-data.txt'
transformed_file = 'transformed.txt'
output_file = 'data_for_analytics.csv'


def extract():
    global input_file
    print("Inside Extract")
    # Read the contents of the file into a string
    with open(input_file, 'r') as infile, \
            open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split(':')
            if len(fields) >= 6:
                field_1 = fields[0]
                field_3 = fields[2]
                field_6 = fields[5]
                outfile.write(field_1 + ":" + field_3 + ":" + field_6 + "\n")


def transform():
    global extracted_file, transformed_file
    print("Inside Transform")
    with open(extracted_file, 'r') as infile, \
            open(transformed_file, 'w') as outfile:
        for line in infile:
            processed_line = line.replace(':', ',')
            outfile.write(processed_line + '\n')


def load():
    global transformed_file, output_file
    print("Inside Load")
    # Save the array to a CSV file
    with open(transformed_file, 'r') as infile, \
            open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(line + '\n')


def check():
    global output_file
    print("Inside Check")
    # Save the array to a CSV file
    with open(output_file, 'r') as infile:
        for line in infile:
            print(line)


# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Your name',
    'start_date': days_ago(0),
    'email': ['your email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'my-first-python-etl-dag',
    default_args=default_args,
    description='My first DAG',
    schedule_interval=timedelta(days=1),
)

# Define the task named execute_extract to call the `extract` function
execute_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag,
)

# Define the task named execute_transform to call the `transform` function
execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag,
)

# Define the task named execute_load to call the `load` function
execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
)

# Define the task named execute_load to call the `load` function
execute_check = PythonOperator(
    task_id='check',
    python_callable=check,
    dag=dag,
)

# Task pipeline
execute_extract >> execute_transform >> execute_load >> execute_check

'''================================== Dag Submission =================='''

'''
Submitting a DAG is as simple as copying the DAG Python file into the dags folder in the AIRFLOW_HOME directory.
Airflow searches for Python source files within the specified DAGS_FOLDER. The location of DAGS_FOLDER can be located in the airflow.cfg file, where it has been configured as /home/project/airflow/dags.
'''

'''
Airflow will load the Python source files from this designated location. It will process each file, execute its contents, and subsequently load any DAG objects present in the file.
Therefore, when submitting a DAG, it is essential to position it within this directory structure. Alternatively, the AIRFLOW_HOME directory, representing the structure /home/project/airflow, can also be utilized for DAG submission.
'''

'''
    Open a terminal and run the command below to set the AIRFLOW_HOME.

    1. export AIRFLOW_HOME=/home/project/airflow
    2. echo $AIRFLOW_HOME

    Run the command below to submit the DAG that was created in the previous exercise.

        1. export AIRFLOW_HOME=/home/project/airflow
        2. cp my_first_dag.py $AIRFLOW_HOME/dags
        
         

Verify that your DAG actually got submitted.

Run the command below to list out all the existing DAGs.

    1. airflow dags list

    

Verify that my-first-dag is a part of the output.

    1. airflow dags list|grep "my-first-dag"

    

You should see your DAG name in the output.

Run the command below to list out all the tasks in my-first-dag.

    1. airflow tasks list my-first-dag

    

You should see 2 tasks in the output.

'''


