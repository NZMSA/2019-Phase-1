# 2019-Phase-1 - Big Data and Analytics

## Introduction
The big data and analytics module will focus on transforming data into insightful information. We will be loading a large dataset to Azure Databricks where we will clean, transform and store the data we manipulate. With the use of Power BI we will then import our transformed data to create useful visualisations.

## Learning Outcomes
- Learn how to create and use an Azure Databricks workspace
- Become comfortable manipulating large datasets
- Access tables from Azure Databricks in Power BI
- Create visualisation using Power BI

## Requirements
- An Azure for Students subscription
- Power BI Desktop Application
    - For those who have a Mac, you will need to create a VM running windows as Power BI is Windows only
- Some basic knowledge on one or more of the following languages (not required but will make things easier):
    - Python, Scala, R, SQL
- Enthusiasm

## Contents

### Part 1 - Azure Databricks
In this subsection we will learn how to set up an Azure Databricks service to perform ETL. We will go through connecting into your Databricks workspace, where we will create a cluster and notebook. We will upload a large dataset and within the created notebook we will use Python (however you can use Scala, R, or SQL if you prefer) to perform operations on it. We will create a configuration which we can then use in Part 2 to connect to Power BI.

### Part 2 - Power BI
In this subsection we will create an account to use PowerBI. We will connect to our Azure Databricks workspace so that we can get the data we transformed in Part 1. We will learn and create various visualisations.

## Assignment
For your assignment, you will need to:
- Select a different dataset. You can use more than one datasets if you like. (Hint: Head over to Kaggle to find a variety of datasets).
- Create an Azure Databricks workspace, create a cluster and notebook
- Load your selected dataset and perform at least 3 or more **useful** transformations on the data. If you decide to do a format revision transformation as per the documentation, please dont just copy the currency conversion example. (Examples of transformation types are in the Part 1 documentation)
- Create and save a new table(s) based on the manipulations you performed above.
- Connect Power BI to your Azure Databricks workspace table(s).
- Create visualisations in Power BI using the above table(s).

### Submission
Submissions will be done through the portal that we will provide to you.

You will need to submit:
- Your public GitHub repo url. This repo should be the following:
    - A text file which contains all the code from your Azure Databricks Notebook
    - CSV file(s) of all the transformed data tables you create
    - Screenshot of your Azure Databricks workspace
    - Screenshots of your visualisations you create on Power BI
- Screenshot(s) of your Microsoft Learn profile dashboard showing your completed modules.
    

### Microsoft Learn videos
1. [Microsoft Learn - Introduction to Azure Databricks](https://docs.microsoft.com/en-us/learn/modules/intro-to-azure-databricks/index)
2. [Microsoft Learn - Create and use analytics reports with Power BI](https://docs.microsoft.com/en-gb/learn/paths/create-use-analytics-reports-power-bi/)


