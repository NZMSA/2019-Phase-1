# Part 1 - Azure Databricks
In this subsection we will learn how to set up an Azure Databricks service to perform ETL. We will go through connecting into your Databricks workspace, where we will create a cluster and notebook. We will upload a large dataset and within the created notebook we will use Python (however you can use Scala, R, or SQL if you prefer) to perform operations on it. We will create a configuration which we can then use in Part 2 to connect to Power BI.

Let's get stared!

## Contents
0. Find a dataset
1. Create an Azure Databricks service
2. Create a Cluster
3. Upload a raw dataset
4. Create a Notebook
5. Read the raw dataset
6. Transformations
7. Create new table(s) with the cleaned data
8. Setup/get config to connect to Power BI
9. Clean up / Deleting the Resource Group / Do this before closing your web browser!!

# Very important message below!
**Azure Databricks can become quite expensive to run (we are lucky we get a 14 day free-trial of the service otherwise you'd be looking at close to $1000 to run per month). However there will still be a cost for the cluster.** With your Azure for Students subscription, you get $100 free to use. In order to not waste this credit without actually utilising the service, i recommend taking a text copy of your notebook text (and saving this to your local computer) and then deleting the service by deleting the resource group before you close your internet browser. When you want to continue your work, then create a new databricks service, reupload your dataset and paste in your copied text from your previous notebook. This way you can be sure you dont run out of credit or get over charged. Also this way the cost should stay within the free $100 you get. If you've entered your credit card details, then this could get charged so please take even extra caution in that case. We wont be responsible for any costs incurred. In no event shall the authors be liable for any claim, damages or other liability, whether in an action of contract, tort, or otherwise, arising from, out of or in connection with this guide. By continuing you agree to this. 😊


# Let's get started

## 0. Find a dataset

As we will be manipulating a dataset, we first need to get one. I recommend signing up to Kaggle.com and looking through their collection of datasets. Find one which interests you and download its associated CSV file. You will need this in a later step.

## 1. Create an Azure Databricks service

First we will need to log in to the Azure portal at portal.azure.com. On the dashboard you will find to your right a green cross with "Create a resource".

![image](img/azure1.jpg)

In the search box, type "azure databrick"

![image](img/azure2.PNG)

Click on Azure Databricks and you should be shown the below. Depending on your subscription, you may need to click the sign up button which displays (looks different to the below screenshot). If required sign up, otherwise click create.

![image](img/azure3.PNG)

We now need to fill in some basic information in order to create our Databricks service. Enter in a workspace name. For subscription, this will likely be your Azure for Students (or similar). The resource group is important for our case. Opt to create a new one. You can name it whatever you like, but be sure you remember what this name is. Reason being, Databricks can become quick costly, so i recommend deleting your resource group (which would also delete your Databricks service) when you wont be using it. This way you can be sure that your $100 wont run out without you actually using it. Deleting resource group will be shown later in this guide.

Ensure you select the Trial, 14 days Free DBUs for the pricing Tier.

Click Create.

![image](img/azure4.PNG)

Once it gets created, you should be redirected to the service. If this doesnt happen, you can search in the Azure search bar with the name you called it to get to this view.

![image](img/azure5.PNG)

Click "Launch Workspace". This will connect you to your Databricks workspace through single sign on. 

![image](img/azure6a.PNG)

Congratulations! You've successfully created your Azure Databricks service.

## 2. Create a cluster

A cluster is a networked computer in Azure which will work to process your data and commands in your Notebook cells.

In order to do things in Databricks, we first need a cluster, so lets create one.

On the main Databricks dashboard, on the left you should note a menu titled "Clusters". Click on this. You should get redirected to a similar screen.


![image](img/azure8.PNG)

Click on "Create Cluster"

Now we need to create a cluster. You can put any name you like for the cluster name. The 3 highlighted items in the screenshot are very key. By having this configuration we will minimise the charge.

By disabling autoscaling, we will have a set number of workers. A worker runs the Spark executors and other services which are required for proper functioning of clusters.

We will also change the terminate after minutes to 30. What this means, is that our cluster will terminate and turn itself off after inactivity of 30 minutes.

Lastly lets change our number of Workers to 1.

![image](img/azure9a.PNG)

The remaining settings should be OK as per the screenshot. Click Create Cluter.

You will note there will be a new enter on the list of Interactive Clusters. Your new cluster will have a loading spinner. This will normally take around 5 minutes to boot up.

Once it has a green dot next to it, it means its up and running and we can move on to the next part.

## 3. Upload a raw dataset

Remember the dataset you saved from step 0? We'll need it now. Back on the Databricks dashboard, click on Data and "Add Data"

![image](img/azure10.PNG)

Now drag your CSV to the File dropzone. For this example, we will be using movies_metadata.csv (a copy can be obtained from [here](https://www.kaggle.com/rounakbanik/the-movies-dataset))

![image](img/azure11.PNG)

![image](img/azure12.PNG)

Click "Create Table with UI". Select your Cluster from the previous step. Click preview table.

![image](img/azure13.PNG)

A preview of your csv file should get loaded. Depending on your file, you may need to select the highlight option if your first row is headers, like in this case.

![image](img/azure14.PNG)

Click Create.

Your raw data has now been uploaded as a table.

## 4. Create a Notebook

In order to interact with the table we will need to create a notebook. A notebook contains cell which can run code, render formatted text or display visualisations.

To create a notebook, click Workspace on the left menu -> on your account click the drop down arrow -> create -> notebook

![image](img/azure15.PNG)

In the dialog which appears, enter a name, select your chosen language (for this im going to use Python), and select your cluster if its not already populated. Click create.

![image](img/azure16.PNG)

Once created, you'll be taken to your notebook, with one cell present. You can create as many cells as you like, however for this example we will stick to a single cell. The cell is where you put in your code. This is where the bulk of your data manipulations will occur.

![image](img/azure17.PNG)

## 5. Read the raw dataset

To do manipulations, we first need to load our table with the raw data to a dataframe which we can then do operations on.

As im using python, i'll start by adding some import statements into my cell. Im importing them all for simiplicity, but ideally you'd just import the ones you need.

```
from pyspark.sql.functions import * 
from pyspark.sql.types import *

```

Next i'll read the movies table which was created and save it to a dataframe.

```
# Read in the movie table.
movies = spark.sql("select * from movies_metadata_csv")

```

We will then display the contents of our dataframe

```
#this displays the dataframe
display(movies)

```

The entire cell should look like:
![image](img/azure18.png)

Lets run this to view the result. On the top right corner of your cell, you should see a play button. This will execute the code within this cell. Press play and run cell.

Note: It may complain or not show if you dont have a cluster. To connect your cluster you created earlier, in the top left, you will note a "Detached" button. Click this and select your cluster. Wait until its green, then press play.

Once complete, you'll see a table which displays the dataframe. If you want to download this, you can click the highlighted drop down. (However we havent done anything with the dataframe yet, so this would be pointless at this moment).

![image](img/azure19.PNG)

## 6. Transformations

Transformations can be a variety of operations. For the assessment, we would like you to use three or more types of transformations. We will be covering five in this example.

So what are transformations? [This webpage](https://www.stitchdata.com/etldatabase/etl-transform/) bullet points a bunch of transformation types. We will be doing a wide range of transformations: Cleaning, Format Revision, Filtering, Splitting, Data Validation. 

After doing this process, what we want to end up with is three tables:
- Movies (transformed with some key columns of interest)
- MovieType (this links movie id and genre)
- MovieCompany (this links movie id and production company)

#### Filtering:

```
#Remove any non numberical runtimes
movies = movies.filter("runtime !=''")

#Remove any non numerical popularities
movies = movies.filter("popularity !=''")

```

#### Data Validation

Data validation is slightly more complex. What we will do is validate that all numerical columns contain numerical values only. There are a few steps to this. First we need to create a udf - user defined function.

```
def is_digit(value):
    if value:
        return value.isdigit()
    else:
        return False

is_digit_udf = udf(is_digit, BooleanType())

```

Followed by using this udf and some more filtering

```
# Remove any movies with no revenue
movies = movies.withColumn("revenue", when(is_digit_udf(movies['revenue']), movies['revenue'])).filter("revenue != '0'")

# Remove any movies with no budget
movies = movies.withColumn("budget", when(is_digit_udf(movies['budget']), movies['budget'])).filter("budget != '0'")

#Remove any non numerical ids
movies = movies.withColumn("id", when(is_digit_udf(movies['id']), movies['id'])).filter("id !=''")

```

#### Format Revision

Here we are converting the budget and revenue to NZD. The 1.46964 was the current exchange rate at time of writing. Other format revision examples, could be converting other units such as mile <-> kilometer etc.

```
# Convert Budget from USD to NZD
movies = movies.withColumn('revenue_nzd', movies["revenue"].cast("float") * 1.46964)

# Convert Revenue from USD to NZD
movies = movies.withColumn('budget_nzd', movies["budget"].cast("float") * 1.46964)
```

#### Cleaning

Here we are cleaning our date to an actual date type to maintain consistency.

```
# Convert the string to datetime
movies = movies.withColumn('release_date', movies["release_date"].cast(DateType()))
```

After these transformations, your notebook cell should look like:

```
from pyspark.sql.functions import * 
from pyspark.sql.types import *

def is_digit(value):
    if value:
        return value.isdigit()
    else:
        return False

is_digit_udf = udf(is_digit, BooleanType())

# Read in the movie table.
movies = spark.sql("select * from movies_metadata_csv")

#Remove any non numerical runtimes
movies = movies.filter("runtime !=''")

#Remove any non numerical popularity's
movies = movies.filter("popularity !=''")

# Remove any movies with no revenue
movies = movies.withColumn("revenue", when(is_digit_udf(movies['revenue']), movies['revenue'])).filter("revenue != '0'")

# Remove any movies with no budget
movies = movies.withColumn("budget", when(is_digit_udf(movies['budget']), movies['budget'])).filter("budget != '0'")

#Remove any non numerical ids
movies = movies.withColumn("id", when(is_digit_udf(movies['id']), movies['id'])).filter("id !=''")

# Convert the string to datetime
movies = movies.withColumn('release_date', movies["release_date"].cast(DateType()))

# Convert Budget from USD to NZD
movies = movies.withColumn('revenue_nzd', movies["revenue"].cast("float") * 1.46964)

# Convert Revenue from USD to NZD
movies = movies.withColumn('budget_nzd', movies["budget"].cast("float") * 1.46964)

#this displays the dataframe
display(movies)

```

Looks good. But lets keep going. We've preped our columns to create our Movies table, but we still need to do some more additional transformations to create our other two tables - MovieType and MovieCompany

#### Splitting
If you look into the Genre column, you'll note its got json entries in here. What we want to do is explode this json, pull out all the genres for each movie, then create a new table which links movie to all its genres.

In order to explode the json entry, we need to state its schema. We can see it has a long for the genre id and a string for the name of the genre.

Then we create a new column which explodes the json, which we call genres_exploded. We then create another column called genre_name to get the genre names, from the exploded column. We have the data we need, so we can remove the genres_exploded using the drop command. 

Lastly we select the movie id column and our genre_name. We add an alias, so that the row is called genres and we save this all to a new dataframe called genre. The below code reflects what i described above. I've added a display line, so you can see the resulting.

```
# Create a table which maps genres to movies
genres_schema = ArrayType(StructType([StructField("id", LongType()),StructField("name", StringType())]))

# Explode the column, get what we need and save this to a new dataframe
genre = movies\
  .withColumn("genres_exploded", explode(from_json("genres", genres_schema)))\
  .withColumn("genre_name", col("genres_exploded.name"))\
  .drop("genres_exploded")\
  .select("id", col("genre_name").alias("genres"))

display(genre)
```

We have created the dataframe we need to create the MovieType table we need. Saving this as a table is covered in the next section, but while displaying it, we can save this as a csv. Lets do that now as this is a requirement of the assessment (you need to submit csv's of the cleaned tables you will create) To save click the highlight drop down in the below screenshot and select download full results. (Note dont click the icon as it doesnt download the full results.)

Output should look like:
![image](img/azure20.PNG)

Lets now do the same to create the MovieCompanies dataframe.

```
# Create a table which maps production companies to movies
companies_schema = ArrayType(StructType([StructField("id", LongType()),StructField("name", StringType())]))

companies = movies\
  .withColumn("companies_exploded", explode(from_json("production_companies", companies_schema)))\
  .withColumn("production_company_name", col("companies_exploded.name"))\
  .drop("companies_exploded")\
  .select("id", col("production_company_name").alias("production_companies"))

display(companies)

```

As this is also a complete dataframe of the table we want to save, lets save this as well.


## 7. Create new table(s) with the cleaned data

We are almost all done. But there is one more thing we need to do to our data. Dataframes are temporary. We need to save these results to a table back into our Databricks workspace. Lets do this now.

Update this code:
```
# Explode the column, get what we need and save this to a new dataframe
genre = movies\
  .withColumn("genres_exploded", explode(from_json("genres", genres_schema)))\
  .withColumn("genre_name", col("genres_exploded.name"))\
  .drop("genres_exploded")\
  .select("id", col("genre_name").alias("genres"))
```

with

```
genre = movies\
  .withColumn("genres_exploded", explode(from_json("genres", genres_schema)))\
  .withColumn("genre_name", col("genres_exploded.name"))\
  .drop("genres_exploded")\
  .select("id", col("genre_name").alias("genres"))\
  .write.mode("overwrite").saveAsTable("movietype")

```

Note we have added an extra line!

What this does is writes the dataframe as a table named "movietype". Mode is in overwrite, so will overwrite any existing tables with this dataframe.

Lets do the same for movie companies.

Change this code:
```
companies = movies\
  .withColumn("companies_exploded", explode(from_json("production_companies", companies_schema)))\
  .withColumn("production_company_name", col("companies_exploded.name"))\
  .drop("companies_exploded")\
  .select("id", col("production_company_name").alias("production_companies"))

```

with:
```
companies = movies\
  .withColumn("companies_exploded", explode(from_json("production_companies", companies_schema)))\
  .withColumn("production_company_name", col("companies_exploded.name"))\
  .drop("companies_exploded")\
  .select("id", col("production_company_name").alias("production_companies"))\
  .write.mode("overwrite").saveAsTable("movieCompany")
```

Awesome, so these two updates will create our two genre and company tables. But there is one last table we need to also create. This is our movie table, with our transformed data.

Firstly - remember the assessment requirements - we need to save all our tables to csv. We have done this already for genre and companies, but not for our movie table. So lets first do this.

Add the following code. This selects the specific columns in our movies dataframe and displays it.

```
display(movies.select("id", "title", "budget_nzd" , "revenue_nzd", "popularity", "runtime"))
```

Run the code and download the full results.

Awesome, now lets save this dataframe to a table. To do this add the following code:

```
movies\
.select("id", "title", "budget_nzd" , "revenue_nzd", "popularity", "runtime")\
.write.mode("overwrite").saveAsTable("movies")

```

Run the cell. 

Woo hoo! Hopefully all the tables have been created. But how do we know? Easy, click the data menu on the left, if everything has gone well, you should be shown 4 tables under the default database.

![image](img/azure21.PNG)

Final Complete code:
```
from pyspark.sql.functions import * 
from pyspark.sql.types import *

def is_digit(value):
    if value:
        return value.isdigit()
    else:
        return False

is_digit_udf = udf(is_digit, BooleanType())

# Read in the movie table.
movies = spark.sql("select * from movies_metadata_csv")

#Remove any non numberical runtimes
movies = movies.filter("runtime !=''")

#Remove any non numerical popularities
movies = movies.filter("popularity !=''")

# Remove any movies with no revenue
movies = movies.withColumn("revenue", when(is_digit_udf(movies['revenue']), movies['revenue'])).filter("revenue != '0'")

# Remove any movies with no budget
movies = movies.withColumn("budget", when(is_digit_udf(movies['budget']), movies['budget'])).filter("budget != '0'")

#Remove any non numberical ids
movies = movies.withColumn("id", when(is_digit_udf(movies['id']), movies['id'])).filter("id !=''")

# Convert the string to datetime
movies = movies.withColumn('release_date', movies["release_date"].cast(DateType()))

# Convert Budget from USD to NZD
movies = movies.withColumn('revenue_nzd', movies["revenue"].cast("float") * 1.46964)

# Convert Revenue from USD to NZD
movies = movies.withColumn('budget_nzd', movies["budget"].cast("float") * 1.46964)

display(movies)

# Create a table which maps genres to movies
genres_schema = ArrayType(StructType([StructField("id", LongType()),StructField("name", StringType())]))

# Explode the column, get what we need and save this to a new dataframe
genre = movies\
  .withColumn("genres_exploded", explode(from_json("genres", genres_schema)))\
  .withColumn("genre_name", col("genres_exploded.name"))\
  .drop("genres_exploded")\
  .select("id", col("genre_name").alias("genres"))\
  .write.mode("overwrite").saveAsTable("movietype")

display(genre)

# Create a table which maps production companies to movies
companies_schema = ArrayType(StructType([StructField("id", LongType()),StructField("name", StringType())]))

companies = movies\
  .withColumn("companies_exploded", explode(from_json("production_companies", companies_schema)))\
  .withColumn("production_company_name", col("companies_exploded.name"))\
  .drop("companies_exploded")\
  .select("id", col("production_company_name").alias("production_companies"))\
  .write.mode("overwrite").saveAsTable("movieCompany")

display(companies)

display(movies.select("id", "title", "budget_nzd" , "revenue_nzd", "popularity", "runtime"))

movies\
.select("id", "title", "budget_nzd" , "revenue_nzd", "popularity", "runtime")\
.write.mode("overwrite").saveAsTable("movies")
```

## 8. Setup/get config to connect to Power BI

So we've done everything we need to do to our data. Now we need to get some items from our databricks workspace so we can use these in Power BI to connect to our tables.

On the top right, you'll notice a person icon. Click it and select user settings.
![image](img/azure22.PNG)

You'll be taken to the tab which mentions Access Token. Click Generate New Token.

![image](img/azure23.PNG)

Name it PowerBI and press generate. You will be given a key. Copy this key down and save it to a text file somewhere on your local computer as you will need this in part 2 and you wont be able to access or view this key after this moment. Press done.

Now we need to get a server url. Click on clusters on the left menu and select your cluster. Click advanced options (highlighted)

![image](img/azure24.PNG)

Then select JDBC/ODBC

![image](img/azure25.PNG)

Scroll down to the JDBC URL text box and copy the highlighted. Copy this to a text file.
![image](img/azure26.PNG)

Then add https: to the start of it. It should look something like (based on above screenshot):

```
https://australiaeast.azuredatabricks.net:443/sql/protocolv1/o/1545214557707270/0413-072247-sail479
```

In part 2 we will go through how to use these two values to connect to your databricks workspace via Power BI.

## 9. Clean up / Deleting the Resource Group / Do this before closing your web browser!!

Its good to delete your databricks service when you wont be using it to prevent being charged unnecessarily. Note if you want to access your tables in PowerBI, you will need to have the service present, so make sure to recreate it when you do the PowerBI bit and delete it again after. 

**Before you do this, make sure you copy all the text from your notebook cell to a local text file, so that you can copy paste it next time you re create your service!!!!**

Go back to your tab which has the Azure Portal Open (or open another tab and go to [azure](portal.azure.com))

In the top search bar, enter in the name of your resource group.

![image](img/azure27.PNG)

Click on the entry under Resource Groups (as highlighted).

![image](img/azure28.PNG)

Click the Delete resource group button.

![image](img/azure29.PNG)

Enter in the name of the resource group and click delete.

![image](img/azure30.PNG)

It should start to delete. This will take some time, to be sure its deleting, click the bell on the top right and you should see a notification that its deleting.

![image](img/azure31.PNG)

### To continue your work next time:
You'll need to do steps: 1, 2, 3, 4, (and 8 if you want to connect to PowerBI this time). Then paste in the text you saved from your last notebook from the text file on your computer. Finally to redo step 9 and delete the resource group once your done again.
