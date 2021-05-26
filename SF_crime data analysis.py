# Databricks notebook source
# MAGIC %md
# MAGIC ## SF crime data analysis

# COMMAND ----------

# MAGIC %md 
# MAGIC **In this notebook, I use Spark SQL for big data analysis on SF crime data from 2003 to 2018/05/15.** 
# MAGIC 
# MAGIC (https://data.sfgov.org/Public-Safety/Police-Department-Incident-Reports-Historical-2003/tmnf-yvry). 
# MAGIC 
# MAGIC The current version is OLAP (Online Analytical Processing) for SF crime data analysis.  
# MAGIC 
# MAGIC (--Possible plans in the future:
# MAGIC 1. unsupervised learning for spatial data analysis.  
# MAGIC 2. time series data analysis.  )

# COMMAND ----------

# DBTITLE 1,Import package 
from csv import reader
from pyspark.sql import Row 
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd
import numpy as np
import seaborn as sb
import matplotlib.pyplot as plt
import warnings

import os
os.environ["PYSPARK_PYTHON"] = "python3"


# COMMAND ----------

# download data from SF gov website

#import urllib.request
#urllib.request.urlretrieve("https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD", "/tmp/myxxxx.csv")
#dbutils.fs.mv("file:/tmp/myxxxx.csv", "dbfs:/laioffer/spark_hw1/data/sf_03_18.csv")
#display(dbutils.fs.ls("dbfs:/laioffer/spark_hw1/data/"))
## link
# https://data.sfgov.org/api/views/tmnf-yvry/rows.csv?accessType=DOWNLOAD


# COMMAND ----------

data_path = "dbfs:/laioffer/spark_hw1/data/sf_03_18.csv"
# use this file name later

# COMMAND ----------

# DBTITLE 1,Get dataframe and sql

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("crime analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sf_crime = spark.read.format("csv")\
  .option("header", "true")\
  .option("interSchema", "true")\
  .load(data_path)

#original dataset
display(sf_crime)


# COMMAND ----------

## function to transform the date
from pyspark.sql.functions import to_date, to_timestamp, hour, month, year
sf_crime = sf_crime.withColumn('IncidntDate', to_date(sf_crime.Date, "MM/dd/yyyy"))
sf_crime = sf_crime.withColumn('Time', to_timestamp(sf_crime.Time, "HH:mm"))
sf_crime = sf_crime.withColumn('Hour', hour(sf_crime['Time']))
#df_opt1 = sf_crime.withColumn("DayOfWeek", date_format(sf_crime.Date, "EEEE"))
sf_crime = sf_crime.withColumn('Year', year(sf_crime['IncidntDate']))
sf_crime = sf_crime.withColumn('Month', month(sf_crime['IncidntDate']))

# dataset after time transformation
# create sql table
sf_crime.createOrReplaceTempView("sf_crime")
display(sf_crime)
sf_crime.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ####0. number of crimes by year

# COMMAND ----------

crime_yearly = spark.sql("select count(*) as yearly_crimes, Year from sf_crime group by 2 order by 2")
display(crime_yearly)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analysis
# MAGIC The line chart above shows the change of yearly crime counts from 2003 to 2018. 
# MAGIC 
# MAGIC From the line chart we can see that 2015 has the highest cirme rate while 2011 are around the lowest crime rate. The number of crimes descreased gradually from 2003 to 2011, where it reached the lowest in 2011. But from 2012 to 2017, the number of crimes rose rapidly. The crime rate was so high in 2015, which may be relevant to the Propostition 47 signed by the governor in the California referendum in 2014 that led to more theft and robbery crimes.
# MAGIC 
# MAGIC We also noticed that 2018 data is far less than other years, which is due to incomplete data with just first 4 months in 2018. So we will look at monthly count below.

# COMMAND ----------

#check the last record of 2018
last = spark.sql("select Date from sf_crime where Year = 2018 and Month = 5 order by Date desc limit 10")
display(last)

# COMMAND ----------

crime_monthly = spark.sql("select count(*) as monthly_crimes, Year, Month from sf_crime group by 2,3 order by 2,3")
display(crime_monthly)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analysis
# MAGIC As can be seen from above, 2018 only contains full records for the first 4 months and records only up to 15th in May. Therefore when comparing yearly difference or monthly difference for May-December, 2018 should be left out.
# MAGIC 
# MAGIC In 2013-2017, the crime rates are above the average of the 15-year range.
# MAGIC 
# MAGIC **Calendar effect:*
# MAGIC 
# MAGIC January, March, May, August and October have seen a rise on crimes from previous month, which may have something to do with one more day in calender. 
# MAGIC 
# MAGIC Feburary have the lowest crime numbers, which is due to the fact that it has 2 or 3 days fewer than other months.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. number of crimes for different category

# COMMAND ----------

crimeCategory = spark.sql("SELECT  category, COUNT(*) AS Count FROM sf_crime GROUP BY category ORDER BY Count DESC")
display(crimeCategory)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analysis
# MAGIC As can be seen from the bar chart, Larceny/Theft is the most committed crime at SF, which is about 60% more than the second crime 'Other offenses'. 
# MAGIC 
# MAGIC As mentioned before, Proposition 47, the 2014 ballot measure that reclassified nonviolent thefts as misdemeanors if the stolen goods are worth less than $950, had emboldened thieves.
# MAGIC 
# MAGIC Other offenses, non-criminal and assault are the second, third and forth of the most committed crimes respectively.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. the number of crimes for different district

# COMMAND ----------

crime_district = spark.sql("SELECT PdDistrict, count(*) as count from sf_crime group by 1 order by 2 desc")
display(crime_district)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analysis
# MAGIC As can be seen from the bar chart, most crimes are committed in Sounthern district. Mission and Northern district follows as the second and third place with crime counts similar to each other. Richmond and Park district have the lowest crime numbers. 
# MAGIC 
# MAGIC Therefore, when deciding where to live, I would suggest to consider those districts with lower crime rate such as Richmond, and avoiding areas close to Southern district. If go traveling or commuting to districts with higher crime rate, keep careful watch for potential danger in your surroundings .

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. the number of crimes each Sunday at "SF downtown".   
# MAGIC SF downtown is defiend  via the range of spatial location. For example, you can use a rectangle to define the SF downtown, or you can define a cicle with center as well. 
# MAGIC San Francisco Latitude and longitude coordinates are: 37.773972, -122.431297. X and Y represents each. So we assume SF downtown spacial range: X (-122.4213,-122.4313), Y(37.7540,37.7740). 
# MAGIC  

# COMMAND ----------

crime_Sun_SFdowntown = spark.sql("""
                                 with sunday_downtown_crime as(
                                 select substring(Date,1,5) as date,
                                        substring(Date,7) as year
                                 from sf_crime 
                                 where (DayOfWeek = 'Sunday' 
                                 and X <= -122.4213 
                                 and X >= -122.4313 
                                 and Y >= 37.7540 
                                 and Y <= 37.7740)
                                 )
                                 select year, date, count(*) as count
                                 from sunday_downtown_crime
                                 group by 1,2 
                                 order by 1,2
                                 """)

display(crime_Sun_SFdowntown)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analysis
# MAGIC The bar chart above shows the history crime rate on a certain day of week(eg.Sunday) at a customized area (eg.SF downtown).
# MAGIC On 06/30, 2013, there was a peak of 54 crimes that Sunday at SF downtown. Police officers could further analyze that abnormal case to gain possible insights on the reason.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.the number of crime in each month of 2015, 2016, 2017, 2018. 

# COMMAND ----------

monthly_crime = spark.sql("""
select Year, Month, count(*) as crime_number from sf_crime
where Year in (2015,2016,2017,2018) group by 1,2 order by 1,2
""")

display(monthly_crime)

# COMMAND ----------

display(monthly_crime)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analysis
# MAGIC 
# MAGIC From the bar chart, we can see that in 2015-2017, January, March and May have most crimes.
# MAGIC August in 2015, October and December in 2016 and 2017 also have more crimes.
# MAGIC 
# MAGIC From the line chart, we can see a descreasing trend of crime rate from fourth quarter of 2015 to October 2016. It was not until the beginning of 2018 that crime rate significantly dropped in the first four months. This might suggest a good job done by the SFPD in 2018.
# MAGIC 
# MAGIC Why there's always more crimes in certain month besides the fact of more days on calendar? To answer this question, I took a look at crimes in December and January by date.

# COMMAND ----------

crime_Dec = spark.sql("select Year, substring(Date,1,5) as date, count(*) as crime_number from sf_crime where Year in (2015,2016,2017,2018) and Month in (12) group by 1,2 order by 1, 2")
display(crime_Dec)

# COMMAND ----------

crime_Jan = spark.sql("select Year, substring(Date,1,5) as date, count(*) as crime_number from sf_crime where Year in (2015,2016,2017,2018) and Month in (1) group by 1,2 order by 1, 2")
display(crime_Jan)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #####Analysis
# MAGIC From the line charts, it seems that days before New Year's Day always has the most crimes, but would drop immediatly after New Year's day. There's also the lowest crimes committed on the exact day of Chritmas. The period after Christmas until New Year's Day has witnessed a growing number of crimes.
# MAGIC 
# MAGIC Therefore, during these periods, the police should arrange more force, despite the fact that it should be holiday off. 
# MAGIC 
# MAGIC (Or is it possible that just because most police officers take breaks during this period and thus reduce the police force, that the criminals become more active and commit more crimes?)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 5.travel suggestion to visit SF
# MAGIC Analyze the number of crime w.r.t the hour in certian day like 2015/6/15, 2016/6/15, 2017/6/15. 

# COMMAND ----------

hourly_crime_certainDay = spark.sql("""
select Date, 
Hour, 
count(*) as crime_number, 
Year
from sf_crime where Date like '06/15/%'
group by 4,1,2 order by 1,2
""")
display(hourly_crime_certainDay)

# COMMAND ----------

#plot by total number
display(hourly_crime_certainDay)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analysis
# MAGIC The first line chart shows hourly crime rate grouped by years, and the second line chart shows the aggregate count across the 14 years from 2003 to 2017.
# MAGIC 
# MAGIC From 2003-2017 counts in total, we can see that on June 15 the highest crime number occurs around 12pm, and there's an increasing trend starting from 1pm and reached the second peak around 3pm. 
# MAGIC 
# MAGIC There're also fewer crimes in the morning than in the afternoon or evening. 
# MAGIC 
# MAGIC So tourists should better travel in the morning, and should be careful after the lunch time.
# MAGIC 
# MAGIC A further thinking: what type of crime should they watch out for? Is there a difference of most frequent types of crime across different hours in a day? 
# MAGIC 
# MAGIC Here I plot line chart for each hour showing hourly trends of different types of crime. 

# COMMAND ----------

hourly_type_certainD = spark.sql("""
select Year, Hour, Date, Category, count(*) as Count
from sf_crime
where Date like '06/15/%'
group by 1,2,3,4 
order by 1,2,4""")
display(hourly_type_certainD)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analysis
# MAGIC 
# MAGIC From the above chart we can see that Larceny/Theft is the most frequent crime from around 11am to 23pm, while Other Offenses is the most frequent crime from 0am to 10am. Therefore, if tourists go out at noon, they should be most careful for their belongings.
# MAGIC 
# MAGIC Notice that not only tourists should be aware of such crimes, but also shop owners as well. Shoplifting epidemic has worsened over the years, which cause so much heavy burden on shop owner that more and more of the shops has closed because the scale of thefts had made business untenable. According to The New York Times, this spikes in organized retail crime is only witnessed in San Franciso but not other cities in the state, which implies the problem lies not in California law but mostlly on SFPD. The police should definitely provide support for the shop owners and other local business.

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 6.advice to distribute the police
# MAGIC (1) Step1: Find out the top-3 danger disrict  
# MAGIC (2) Step2: find out the crime event w.r.t category and time (hour) from the result of step 1  

# COMMAND ----------

#step1:
top3_dangerDistrict = spark.sql("select PdDistrict, count(*) as crime_number from sf_crime group by 1 order by 2 desc limit 3")
display(top3_dangerDistrict)

# COMMAND ----------

#step2:
crime_event = spark.sql("""
select PdDistrict, Category, Hour , count(*) as count
from sf_crime 
where PdDistrict in ('SOUTHERN', 'MISSION', 'NORTHERN')
group by 1, 2, 3
order by 1, 2, 3
""")
display(crime_event)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #####Analysis
# MAGIC 
# MAGIC The top 3 dangerous districts are Southern, Mission and Northern.
# MAGIC 
# MAGIC (See the plot in Tableau)
# MAGIC https://public.tableau.com/profile/zishan.cheng#!/vizhome/sf_crimetop3districthourlycrimecategory/Sheet1?publish=yes
# MAGIC 
# MAGIC Here I assume that SF police force is arranged as a whole and each district also has some of its own force, so we need to distribute force across districts for SFPD, as well as across hours for each district's police office.
# MAGIC 
# MAGIC From the stacked bar chart in Tableau we can see that the highest number of crimes occurs at 6pm and 12pm. Additionally, the 5 most committed crimes in each district are larceny/theft, other offenses, non-criminal, assault and durg/narcotic, which accounts for over half of the crimes.
# MAGIC 
# MAGIC 
# MAGIC In the Southern district, the period from 11am to 12am has crime number above average of a day within that district. In Mission district, the period is from 11am to 12am. In Northern district, the period is from 12pm to 12am. So more police force should be distributed to these districts during these time periods. 

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 7. hints to adjust policy on different type of crime
# MAGIC For different category of crime, find the percentage of resolution. 

# COMMAND ----------

# DBTITLE 1,see the types of resolution
# MAGIC %sql select distinct(resolution) as resolve from sf_crime

# COMMAND ----------

crime_total = spark.sql("select Category, count(*) as total from sf_crime group by Category order by 2 desc")
crime_total.createOrReplaceTempView("crime_total")
display(crime_total)
crime_total.cache()

# COMMAND ----------

crime_res = spark.sql("select Category, count(*) as resolved from sf_crime where Resolution != 'NONE' group by 1")
crime_res.createOrReplaceTempView("crime_res")
display(crime_res)

# COMMAND ----------

resolve_rate = spark.sql("select a.Category, resolved, total, resolved/total as percentage from crime_total a left join crime_res b on a.Category = b.Category order by percentage desc")
display(resolve_rate)

# COMMAND ----------

resolve = spark.sql("""
with resolve as (select Category, resolution, count(*) as tally
from sf_crime group by 1,2)
select a.Category, a.resolution, a.tally, b.total 
from resolve a 
left join (select * from crime_total) b
on a.Category = b.Category 
order by b.total desc, a.tally desc
""")
display(resolve)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Analysis
# MAGIC 
# MAGIC From the bar chart above showing the percentage of resolution rate, we can see that prostitution, warrants and driving under the influence has the highest rates of resolution among all 37 categories, with resolution rates above 94%. 
# MAGIC 
# MAGIC However, from the other side of the bar chart, we can see that recovered vehicle, vehicle theft, larceny/theft has the lowest rats of resolution below 10%, followed by suspicious ooc, vandalism, with resoluation rates both below 15%. 
# MAGIC 
# MAGIC Why are so many of these types of crimes remain unresolved? Is it due to a lack of police capability or legal restrictions on these types? I believe this question should be answered by both parties. 
# MAGIC 
# MAGIC From the side of police department, I suggest them investigate into those unresolved cases, to see if there's a problem of laissez-faire attitute or ability. If there's a downplay on those cases, for example, taking it for granted that theft is hard to traced and thus making no effort to resolve it, then the police should rectify the situation. If there's a need for applying advanced techniques or skills, the department in charge should fix it. 
# MAGIC 
# MAGIC As for the policy makers, I suggest them investigate on the motivation behind those crime. If there's a difficulty for the police to resolve those cases (for either reasons), then it would be best to resolve it before it happens by adjusting relevant policies.
# MAGIC 
# MAGIC Notice that larceny/theft is the most committed crime, vandalism at 7th place, vehicle theft at 8th. There should be placed more emphasis on these types of crime with least resolution rate, either to enhance the police's technique/skills for solving such crimes or enforce more restrictions/punishments on those crimes.
# MAGIC 
# MAGIC The second pie chart demonstrates the resolution records of top 4 most committed crimes, so as to provide more directly visualization comparing crime number and resolution rate. 
# MAGIC 
# MAGIC As can be seen, the resolution rate is shockingly low for these most frequent crimes with more than 60% of them unresolved in three of the four most frequent crimes. So there's really an urgent need to improve this situation.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusion

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Motivation
# MAGIC 
# MAGIC When considering about whether to reside in a certain place or not, one of the most important factors is to research on the safety and crime rate of the area. Most people would prefer a safer place to live, especially for a mid- or long-term residence, or someone plan to run their own business. 
# MAGIC 
# MAGIC For every new UC Berkeley student coming from other area, he/she must have heard about the safety concerns around the campus: drug, homeless people, etc. As an international student from even another country, I remember the feeling of anxiety and uncertainty over the potential safety issue. Even though I have been here for over 1 year and completed my study safe and sound, I thought it would still be helpful to analyze the crime data, in an attempt  to understand what's going on around this place and how could residents/tourists/shop owners stay cautious and how could policies be further enhanced to improve the police force. 
# MAGIC 
# MAGIC I choose SF data because it is a world-famous metropolis with a population over 880K, and would also hopefully be the next place I were about to live.

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Methods
# MAGIC 
# MAGIC 1) Data processing
# MAGIC 
# MAGIC   To begin with, in order to perform analysis on SF crime data from 2003-2018 of a large size more than 551MB, I utilized **Spark SQL** to load and manipulate the dataset. 
# MAGIC 
# MAGIC   The reason is that Spark offers a parallel method to process the data and thus leads to a much faster running speed than using Python pandas or Hadoop. **Spark SQL can directly read from multiple sources and ensures fast execution of existing Hive queries**. Moveover, **SQL can provide a easy way to load data without checking the many APIs from PySpark**, so it is a good way to get start. **After using Spark SQL to load data, one can use PySpark DataFrame format to get and transform the data into some representations that can be used to train machine learning model in the future.**
# MAGIC On deciding to use Spark SQL instead of PySpark DataFrame method, I compared the performance of both methods and noticed that SQL was faster.
# MAGIC 
# MAGIC 2) Data visualization
# MAGIC 
# MAGIC   After data cleaning and processing, I utilized the built-in visualization tool in Datacricks by display() function for basic visualization need, as well as **Tableau** for more advanced demonstration purpose. I selected the features of interest such as categories, time, districts of crimes to visualize so that I could better understand and analyze the potential properties and relations among different features. 
# MAGIC   
# MAGIC   I also chose the most suitable form of plot from various types in order to fulfill the analysis need. For example, I used a **line chart and bar chart of month vs crime number and grouped lines by different year, so as to display clearly the variation of crime rate within one year and also across different years**. I also used **pie charts of percentage of resolution in each category of crime and sorted by crime frequency, showing clearly the resolution condition of the most frequent crime**. 
# MAGIC   
# MAGIC   When displaying the crime event w.r.t category and time (hour) in top 3 dangerous district, I plotted a **stacked bar chart combining with line chart and divided by districts, so that it is easy to compare hourly difference of number and type of crimes as well as that across different districts**.
# MAGIC   
# MAGIC How to select the best way for visualization makes a huge difference and is of great importance in conveying the insights from data analysis results.
# MAGIC   

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Analysis and insights
# MAGIC 1. From the bar chart and line chart of count of crime in 2015-2018, I noticed a decreasing crime in 2018. From the charts of 15 years, I found an increasing trend of crime since 2011, but the first four months of 2018 so far has the lowest number of crime over the years.
# MAGIC 
# MAGIC 2. From the bar chart of hour vs count of crime of different categories, and line plot of hour vs count of crime in a certain day, I found that most of crimes occur during lunch or in the afternoon in 14-22pm, and early morning has the least amount of crime, especially in 3-7am. So it is suggested to travel in the morning and assign more police in the evening.
# MAGIC 
# MAGIC 3. From the bar chart and pie chart of categories vs resolution rate, Prostitution, Warrants and Driving under influence are most resolved, while Recovered vehicle, Vehicle theft and Larceny/theft are least resolved. Moveover, some categories with the lowest resolution rate are also the most frequently committed crimes, which should be fixed urgently and paid more effort and attention to by the police and policy makers.
# MAGIC 
# MAGIC 
# MAGIC   
# MAGIC   
# MAGIC   
# MAGIC   

# COMMAND ----------


