# SF-crime-analysis-in-Apache-Spark

Please use link to the pulished notebook:

https://databricks-prod-cloudfront.cloud.databricks.com/public/4027ec902e239c93eaaa8714f173bcfc/5389513496392988/1632744863072986/3093881762394895/latest.htmlI

**In the Databricks notebook, I use Spark SQL for big data analysis on SF crime data from 2003 to 2018/05/15.** 

The current version is OLAP (Online Analytical Processing) for SF crime data analysis.  

(--Possible plans in the future:
1. unsupervised learning for spatial data analysis.  
2. time series data analysis.  )

#### 1. Motivation

When considering about whether to reside in a certain place or not, one of the most important factors is to research on the safety and crime rate of the area. Most people would prefer a safer place to live, especially for a mid- or long-term residence, or someone plan to run their own business. 

For every new UC Berkeley student coming from other area, he/she must have heard about the safety concerns around the campus: drug, homeless people, etc. As an international student from even another country, I remember the feeling of anxiety and uncertainty over the potential safety issue. Even though I have been here for over 1 year and completed my study safe and sound, I thought it would still be helpful to analyze the crime data, in an attempt  to understand what's going on around this place and how could residents/tourists/shop owners stay cautious and how could policies be further enhanced to improve the police force. 

I choose SF data because it is a world-famous metropolis with a population over 880K, and would also hopefully be the next place I were about to live.


#### 2. Methods

1) Data processing

  To begin with, in order to perform analysis on SF crime data from 2003-2018 of a large size more than 551MB, I utilized **Spark SQL** to load and manipulate the dataset. 

  The reason is that Spark offers a parallel method to process the data and thus leads to a much faster running speed than using Python pandas or Hadoop. **Spark SQL can directly read from multiple sources and ensures fast execution of existing Hive queries**. Moveover, **SQL can provide a easy way to load data without checking the many APIs from PySpark**, so it is a good way to get start. **After using Spark SQL to load data, one can use PySpark DataFrame format to get and transform the data into some representations that can be used to train machine learning model in the future.**
On deciding to use Spark SQL instead of PySpark DataFrame method, I compared the performance of both methods and noticed that SQL was faster.

2) Data visualization

  After data cleaning and processing, I utilized the built-in visualization tool in Datacricks by display() function for basic visualization need, as well as **Tableau** for more advanced demonstration purpose. I selected the features of interest such as categories, time, districts of crimes to visualize so that I could better understand and analyze the potential properties and relations among different features. 
  
  I also chose the most suitable form of plot from various types in order to fulfill the analysis need. For example, I used a **line chart and bar chart of month vs crime number and grouped lines by different year, so as to display clearly the variation of crime rate within one year and also across different years**. I also used **pie charts of percentage of resolution in each category of crime and sorted by crime frequency, showing clearly the resolution condition of the most frequent crime**. 
  
  When displaying the crime event w.r.t category and time (hour) in top 3 dangerous district, I plotted a **stacked bar chart combining with line chart and divided by districts, so that it is easy to compare hourly difference of number and type of crimes as well as that across different districts**.
  
How to select the best way for visualization makes a huge difference and is of great importance in conveying the insights from data analysis results.
  
  
#### 3. Analysis and insights
1. From the bar chart and line chart of count of crime in 2015-2018, I noticed a decreasing crime in 2018. From the charts of 15 years, I found an increasing trend of crime since 2011, but the first four months of 2018 so far has the lowest number of crime over the years.

2. From the bar chart of hour vs count of crime of different categories, and line plot of hour vs count of crime in a certain day, I found that most of crimes occur during lunch or in the afternoon in 14-22pm, and early morning has the least amount of crime, especially in 3-7am. So it is suggested to travel in the morning and assign more police in the evening.

3. From the bar chart and pie chart of categories vs resolution rate, Prostitution, Warrants and Driving under influence are most resolved, while Recovered vehicle, Vehicle theft and Larceny/theft are least resolved. Moveover, some categories with the lowest resolution rate are also the most frequently committed crimes, which should be fixed urgently and paid more effort and attention to by the police and policy makers.


  
  
  
