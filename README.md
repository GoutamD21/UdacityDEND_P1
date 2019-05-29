# Introduction :
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app.

## Structure of the Data :

**User Activity Data** : Directory of JSON Logs from the users of the app
**Song Medata** : Directory of JSON Metadata about songs on the platform

**Purpose** : Be able tp perform Analysis based on about data to better understand user behaviour on the app and listening patterns.

## Solution Outlined :
Create **Dimensions** tables on the database that will contain information about the Songs, Artists and Users.
Create **Fact** table that will contains the the session information and listening history of the user. 
Be able to correctly parse information from the log files such that accurate facts can be derived.

## Technologies :  
**Database** - Postgresql 11.2
**Language** - Python 3.7
**IDE** - Jupyter Notebook
**Libraries** : pandas, psycopg2, sql

## Steps :

Create database and tables on Postgres using python script that will drop databases and tables everytime it runs.
Specific Create table statements where created in an another script that will be invoked using the first script. Certain rules about create table statements were followed including **NOT NULL** contraints and **PRIMARY KEY**.
A data pipeline script that will navigate to the folder structure to fetch the JSON files into Data Frames and Insert into tables. Insert into tables will also handle Conflicts by either updating information or by doing nothing.
The JSON data was filtered from both the File as well as after loading into a DataFrame. 

## Table Structures :

## Songplays
![songplay table](https://imgur.com/DDAqBZV.png)

## Users
![Users table](https://imgur.com/UydZLK9.png)

## Songs
![Song Table](https://imgur.com/FLzUjnD.png)

## Artists
![Artist Table](https://imgur.com/3OmygrW.png)

## Time
![Time table](https://imgur.com/Giyjfar.png)

