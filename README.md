# STEDI human balance analytics

(Udacity AWS Data ENgineering Nanodegree project)

## Goal

Using AWS Glue, AWS S3, Python, and Spark, this project build a lakehouse solution in AWS using Python scripts, that satisfies these requirements from the STEDI data scientists.

## Landing zone 

To simulate the data coming from the various sources, we  create our own S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones, and copy the data there as a starting point. Once arrived there, we will qiery the data using Athena.

## Glue jobs

### Step 1: Creating a trusted zone

The Data Science team has done some preliminary data analysis and determined that the Accelerometer Records each match one of the Customer Records. Therefore, we will create 2 AWS Glue Jobs that do the following:

- Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called `customer_trusted`.
- Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called `accelerometer_trusted`.

Again, once this step is done, the data can be querie using Athena.

### Step 2: Creating a curated zone

Data Scientists have discovered a data quality issue with the Customer Data. The serial number should be a unique identifier for the STEDI Step Trainer they purchased. However, there was a defect in the fulfillment website, and it used the same 30 serial numbers over and over again for millions of customers! Most customers have not received their Step Trainers yet, but those who have, are submitting Step Trainer data over the IoT network (Landing Zone). The data from the Step Trainer Records has the correct serial numbers.

The problem is that because of this serial number bug in the fulfillment data (Landing Zone), we don’t know which customer the Step Trainer Records data belongs to. THerefore, we will write the following Glue job: 

- Sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated.

### Step 3: Prepare for research data analysis

Finally, we need two more Glue jobs to use only step data for customers who agreed to have their data used for research:

- Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called `step_trainer_trusted`` that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
- Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.
