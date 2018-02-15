# kf's attempt at getting this to run

## Creating a Virtual Environment

Google Cloud Dataflow requires Python 2.7. To create a virtual environment using Python 2.7, run

```
virtualenv --python=/usr/bin/python2.7 venv
```

Then, run

```
source venv/bin/activate
```

to activate the virtual environment, and run

```
pip install -r requirements.txt
```

to install the requirements.

## Running on Google Cloud Dataflow

To run this script with Dataflow, you will need to run this command in your activated virtual environment:

```
python pipeline.py --requirements_file requirements.txt --project <PROJECT-NAME> --job_name <JOB-NAME> --staging_location gs://pycaribbean/staging_location --temp_location gs://pycaribbean/temp_location --runner DataflowRunner --save_main_session True
```
