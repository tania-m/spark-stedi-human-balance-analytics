# About the initial data

This folder should contain public project starter code.

Upload the directories to your S3 bucket and use them to create your Glue tables.

When you have completed this project, your S3 bucket should have the following directory structure:

```
customer/
- landing/
- trusted/
- curated/
accelerometer/
- landing/
- trusted/
step_trainer/
- landing/
- trusted/
- curated/
```

**Note:** `step_trainer/curated/` contains the data files for the `machine_learning_curated` table.

## Customer records

From JSON record stored in S3.
Containing the following fields:

- serialnumber
- sharewithpublicasofdate
- birthday
- registrationdate
- sharewithresearchasofdate
- customername
- email
- lastupdatedate
- phone
- sharewithfriendsasofdate

## Step trainer records

From JSON record stored in S3.
Containing the following fields:

- sensorReadingTime
- serialNumber
- distanceFromObject

## Accelerometer records

From JSON record stored in S3.
Containing the following fields:

- timeStamp
- user
- x
- y
- z
