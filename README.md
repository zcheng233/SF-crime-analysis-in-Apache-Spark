# SF-crime-analysis-in-Apache-Spark

In this notebook, I performed data and spatial analysis on a 15-year SFPD crime dataset of size over 551MB. The dataset I used is historical SFPD incident reports data from 2003 to 2018/05/15.

In Part 1, I conducted big data OLAP (Online Analytical Processing) based on Spark RDD, DataFrame, and Spark SQL. I compared variation in crime rates and resolution percentage of different categories across districts over time, conducted data visualiazation by seaborn, matplotlib, pyplot and Tableau, analyzed results and provided suggestions for the traveler, policymaker, and police arrangement.

In Part 2, I conducted spatial data analysis and built K-means and bisecting K-means clustering model to explore and visualize the variation of the spatial distribution of larcency/theft and robbery.

A quick look at the clustering visualization:
- Larceny/theft
![theft](clustering_on_theft.png)
- Robbery
![robbery](clustering_on_robbery.png)
