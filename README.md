# SpotiReport

This GitHub repository is dedicated to a periodic updating report that leverages the Spotify API to gather data on song popularity ranks and audio feature scores from popular playlists on Spotify. The collected data is processed and stored in Snowflake, and the insights are presented in a Power BI report. This repository serves as a comprehensive solution for tracking and visualizing trends in popular music across various Spotify playlists.

The featured playlist that we are tracking for this effort are 
* Top 50 USA Daily (https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp)
* Top 50 Global Daily (https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF)
* Top 50 USA Weekly (https://open.spotify.com/playlist/37i9dQZEVXbLp5XoPON0wI)
* Top 50 Global Weekly (https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF)
* Top 50 USA Viral (https://open.spotify.com/playlist/37i9dQZEVXbKuaTI1Z1Afx)
* Top 50 Global Viral (https://open.spotify.com/playlist/37i9dQZEVXbLiRSasKsNU9)

Access Report at: [SpotiReport](https://app.powerbi.com/view?r=eyJrIjoiYTkzMWViZmItZDM4MS00MjkxLWFjNDEtNzU3M2IxYTRkZmRlIiwidCI6IjE3ZjFhODdlLTJhMjUtNGVhYS1iOWRmLTlkNDM5MDM0YjA4MCIsImMiOjF9)

## Report Walkthrough
![image](https://github.com/saldanhad/SpotiReport/blob/main/Miscellaneous/powerbitutorial.gif?raw=true)

## Data Flow diagram
* The new data is first gathered and stored in an azure blob storage via periodic run script in Azure Databricks.
* Next, we make you of Azure Event grid to gather Azure storage blob queues when a new file is uploaded.
* Snowpipe is triggered as this queue is consumed in Snowflake and via the AZURE_STAGE procedure the new data is copied into a stagging table.
* Once in the staging table various tasks are triggered based on the Task tree highlighted in the previous steps. 
* Upon completion of the necessary ETL processes new data UPSERTED/INSERTED into the required dim and fact tables and views are updated. The Power BI report has a real-time connection to the warehouse via Direct Query.
![image](https://github.com/saldanhad/SpotiReport/blob/main/Miscellaneous/dataflow%20diagram.png?raw=true)

## Task Tree
* A – Update/Upsert records to DIM_ARTIST and DIM_ALBUMS tables
* B – Update DIM_SONGS table
* C – Update/Upsert DIM_AUDIOFEATURES table
* D – Load data into transactions snapshot fact table
* C – Truncate Stage table

![image](https://github.com/saldanhad/SpotiReport/blob/main/Miscellaneous/task_tree.png?raw=true)


## ER Diagram
![image](https://github.com/saldanhad/SpotiReport/blob/main/Miscellaneous/ERdiagram.drawio.png?raw=true)

## Data Warehouse Structure
![image](https://github.com/saldanhad/SpotiReport/blob/main/Miscellaneous/DW_arch.jpg?raw=true)

## Power BI Data Model
![image](https://github.com/saldanhad/SpotiReport/blob/main/Miscellaneous/powerbidatamodel.jpg?raw=true{)


### Note: Detailed documentation in attached docx file under miscellaneoua folder.
