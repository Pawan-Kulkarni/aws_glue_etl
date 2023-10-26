<h1> Project Introduction: STEDI Human Balance Analytics </h1>
In this project,  as a data engineer for the STEDI team to build a data lakehouse solution for sensor data that trains a machine learning model.

## AWS Services used:
<ol>
<li>AWS GLUE</li>
<Li>AWS IAM</Li>
<li>AWS Athena</li>
<li>AWS S3</li>
</ol>

<h2>Project Details</h2>
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that trains the user to do a STEDI balance exercise
and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
has a companion mobile app that collects customer data and interacts with the device sensors.
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

Project Summary
As a data engineer on the STEDI Step Trainer team, we'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model.

## Project Data
STEDI has three JSON data sources to use from the Step Trainer. Check out the JSON data in the following folders in the Github repo:

<ol>
<li>customer</li>
<li>step_trainer</li>
<li>accelerometer</li>
</ol>

## 1. Customer Records
This is the data from fulfillment and the STEDI website.

contains the following fields:

<ol>
<li>serialnumber</li>
<li>sharewithpublicasofdate</li>
<li>birthday</li>
<li>registrationdate</li>
<li>sharewithresearchasofdate</li>
<li>customername</li>
<li>email</li>
<li>lastupdatedate</li>
<li>phone</li>
<li>sharewithfriendsasofdate</li>
</ol>


## 2. Step Trainer Records
This is the data from the motion sensor.
contains the following fields:
<ol>
<li>sensorReadingTime</li>
<li>serialNumber</li>
<li>distanceFromObject</li>
</ol>

## 3. Accelerometer Records
This is the data from the mobile app.
contains the following fields:
<ol>
<li>timeStamp</li>
<li>user</li>
<li>x</li>
<li>y</li>
<li>z</li>
</ol>

## Workflow

<ol>
<li>Move customer data into trusted zone by only including those customers who have agreed to share their data. This is done by querying non null sharedWithResearchAsOfDate</li>
<li>Move accelerometer landing data zone to trusted data zone by storing only those data for customers who have agreed to share their reading data. This is done by inner joining the customer_trusted data with the accelerometer_landing data by emails</li>
<li>Move customer trusted data zone to curated data zone by only including customers who have accelerometer data and have agreed to share their data for research called customers_curated. This is done by inner joining the customer_trusted data with the accelerometer_trusted data by emails.</li>
<li>Move step trainer data from landing to trusted zone which contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research.  This is done by inner joining the step_trainer_landing data with the customer_curated data by serial numbers</li>
<li>Move the step trainer trusted data to curated - called as 'machine_learning_curated' ( this data is used for training ML models) , by joining acclerometer_trusted and step_trainer_trusted on timestamps.</li>
</ol>

Data for this project is provided in Data directory.
