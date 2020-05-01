#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd 
import numpy
import matplotlib.pyplot as plt 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


# In[2]:


spark = SparkSession     .builder     .appName("Pysparkexample")     .config("spark.some.config.option", "some-value")     .getOrCreate()


# In[6]:


df1 = spark.read.csv('/data/tripdata/sb.csv', header='true', inferSchema = True)


# In[7]:


df2 = spark.read.csv('/data/tripdata/sc.csv', header='true', inferSchema = True)


# In[8]:


df1.createOrReplaceTempView('sb')


# In[9]:


df2.createOrReplaceTempView('sc')


# In[10]:


split_col = F.split(df1['starttime'],' ')


# In[11]:


df1_date = df1.withColumn('StartDate',split_col.getItem(0))


# In[12]:


df1_new = df1_date.withColumn('StartTime',split_col.getItem(1))


# In[13]:


split_col1 = F.split(df1_new['stoptime'],' ')


# In[14]:


df1_date1 = df1_new.withColumn('StopDate',split_col1.getItem(0))


# In[15]:


df1_latest = df1_date1.withColumn('StopTime',split_col1.getItem(1))


# In[33]:


df1_latest.createOrReplaceTempView('Bike_status')


# In[34]:


# QUERY 1

# This query shows all the records where women passengers, who are subscribers, made trips between 12am - 7am

# This attributes displayed are bikeid, start date, start time, source, stop time, destination and trip duration 



spark.sql('''

select bikeid, StartDate, StartTime,`start station name` as source, 
StopTime, `end station name` as destination, tripduration as duration
from Bike_status 
where StartTime in (
select StartTime 
from (select startDate, bikeid, StartTime
    from Bike_status 
    where StartTime > "00:00:00.000" and StartTime < "07:00:00.000"
    and gender=2 
    and usertype="Subscriber"
    group by StartDate, bikeid, StartTime 
    order by StartDate))


''').show()


# In[18]:


# QUERY 2

# top 5 riders making maximum number of trips in a day in Bluebikes
spark.sql('''select (count(*)) as number_of_trips, bikeid as id 
from sb 
group by bikeid 
order by number_of_trips desc 
limit 5''').show()


# In[19]:


# QUERY 3

# this query shows the locations where max roundtrips were by the biker who took the maximum roundtrips
spark.sql("select `start station name`, `end station name` from sb where bikeid in (select bikeid from (select * from (select count(*) as monthly_roundtrip, bikeid from(select from_unixtime(CAST(unix_timestamp(starttime)/3600 AS int)*3600) as start,bikeid from sb where ((starttime > '2020-01-01 00:00:00' and starttime < '2020-01-31 11:59:59') and `start station name` = `end station name`)group by CAST(unix_timestamp(starttime)/3600 AS int), bikeid order by bikeid asc) group by bikeid order by monthly_roundtrip desc) limit 1)) and `start station name`=`end station name`  ").show()


# In[20]:


# QUERY 4

# shows the number of subscribers 
spark.sql('''select count(*) as Bsub from sb where usertype="Subscriber"''').show()


# In[23]:


spark.sql('''
select `start station name`, count(*) as num_trips, avg(tripduration
) as avg_duration_seconds, min(tripduration
) as min_duration_seconds, max(tripduration
) as max_duration_seconds from sb 
group by `start station name`
''').show()


# In[53]:


#5. Display Bikeids at different start locations.
spark.sql('''select bikeid from Bike_status where `start station name` in (select distinct(`start station name`) from Bike_status)''').show()


# In[35]:


#6. Display bikeids with trip duration greater than the threshold.
spark.sql('''select bikeid from Bike_status 
where tripduration > (select avg(tripduration) as duration from sb1) 
group by bikeid''').show()


# In[38]:


#7. Display Peak hours of a day
spark.sql('''select from_unixtime(CAST(unix_timestamp(starttime)/3600 AS int)*3600), count(*) 
from sb group by CAST(unix_timestamp(starttime)/3600 AS int) 
order by count(*) desc''').show(5)


# In[39]:


# 8. Display the average, Minimum and maximum trip duration per start station.
spark.sql('''
select `start station name`, count(*) as num_trips, avg(tripduration
) as avg_duration_seconds, min(tripduration
) as min_duration_seconds, max(tripduration
) as max_duration_seconds from sb 
group by `start station name`
''').show()


# In[43]:


pip install ipyleaflet


# In[47]:


from ipyleaflet import Map, Heatmap
from random import uniform
m = Map(center=(42.3733121258, -71.0410200806), zoom=13)

bike_lat_lng_df = spark.sql('''select `start station latitude`, `start station longitude` from Bike_status where bikeid = 30326''')
bike_locations = list()
for row in bike_lat_lng_df.rdd.collect():
    bike_locations.append((row["start station latitude"], row["start station longitude"], 89)) # lat, lng, intensity
    
heatmap = Heatmap(
    locations=bike_locations,
    radius=20
)
m.add_layer(heatmap);
m


# In[ ]:





# In[ ]:




