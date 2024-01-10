# SpotiReport

This GitHub repository is dedicated to a periodically updating report that leverages the Spotify API to gather data on song popularity ranks and audio feature scores from popular playlists on Spotify. The collected data is processed and stored in Snowflake, and the insights are presented in a Power BI report. This repository serves as a comprehensive solution for tracking and visualizing trends in popular music across various Spotify playlists.

The featured playlist that we are tracking for this effort are 
* Top 50 USA Daily (https://open.spotify.com/playlist/37i9dQZEVXbLRQDuF5jeBp)
* Top 50 Global Daily (https://open.spotify.com/playlist/37i9dQZEVXbMDoHDwVN2tF)
* Top 50 USA Weekly (https://open.spotify.com/playlist/37i9dQZEVXbLp5XoPON0wI)
* Top 50 Global Weekly (https://open.spotify.com/playlist/37i9dQZEVXbNG2KDcFcKOF)
* Top 50 USA Viral (https://open.spotify.com/playlist/37i9dQZEVXbKuaTI1Z1Afx)
* Top 50 Global Viral (https://open.spotify.com/playlist/37i9dQZEVXbLiRSasKsNU9)

Access Report at: [SpotiReport](https://app.powerbi.com/view?r=eyJrIjoiZWViYWQ4YTYtZDEyZS00M2ZiLTg4ODgtZTg2ZGY1MDAzNDE1IiwidCI6IjE3ZjFhODdlLTJhMjUtNGVhYS1iOWRmLTlkNDM5MDM0YjA4MCIsImMiOjF9)

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

<img src="https://github.com/saldanhad/SpotiReport/raw/main/Miscellaneous/task_tree.png" alt="image" width="1000" height="500">



## STAR Schema
<img src="https://github.com/saldanhad/SpotiReport/blob/main/Miscellaneous/ERdiagram.drawio.png?raw=trueg" alt="image" width="800" height="1000">

## Data Warehouse Structure
![image](https://github.com/saldanhad/SpotiReport/blob/main/Miscellaneous/DW_arch.jpg?raw=true)

## Power BI Data Model
![image](https://github.com/saldanhad/SpotiReport/blob/main/Miscellaneous/powerbidatamodel.jpg?raw=true{)


## DAX functions used

```dax
//create KPI table
KPItable = 
SUMMARIZECOLUMNS (
    'TRANSFACT_LASTTENDAYS'[song_id],
    "current popularity", TRANSFACT_LASTTENDAYS[Curr Popularity],
    "prev popularity", TRANSFACT_LASTTENDAYS[Prev Popularity],
    "KPI", TRANSFACT_LASTTENDAYS[Popularity KPI color],
     "KPI Color",
    IF(TRANSFACT_LASTTENDAYS[Popularity KPI color] = "#009900","Green(Increased)",
      IF(TRANSFACT_LASTTENDAYS[Popularity KPI color]= "#ff0000","Red(Decreased)",
            IF(TRANSFACT_LASTTENDAYS[Popularity KPI color] = "#0000ff", "Blue(Unchanged)")
        )
    )
)

//created data table for artist url
ImageURL = SUMMARIZECOLUMNS (
    'DIM_ARTIST'[ARTIST_ID],'DIM_ARTIST'[ARTIST_IMAGE_URL]
    )

//current popularity score measure
Curr Popularity = 
CALCULATE(
    MAXX(
        FILTER(
            FILTER(
                TRANSFACT_LASTTENDAYS,
                TRANSFACT_LASTTENDAYS[EFFECTIVE_DATE] = MAX(TRANSFACT_LASTTENDAYS[EFFECTIVE_DATE])
            ),
            TRANSFACT_LASTTENDAYS[POPULARITY] <> 0
        ),
        TRANSFACT_LASTTENDAYS[POPULARITY]
    )
)

//prev popularity score measure
Prev Popularity = 
CALCULATE(
    MAXX(
        FILTER(
            TRANSFACT_LASTTENDAYS,
            TRANSFACT_LASTTENDAYS[EFFECTIVE_DATE] = MAX(TRANSFACT_LASTTENDAYS[EFFECTIVE_DATE])- 1
        ),
        TRANSFACT_LASTTENDAYS[POPULARITY]
    )
)

//Popularity KPI Color measure
Popularity KPI color = 
VAR CurrentPopularity = TRANSFACT_LASTTENDAYS[Curr Popularity]
VAR PrevPopularity = TRANSFACT_LASTTENDAYS[Prev Popularity]
RETURN
    SWITCH(
        TRUE(),
        CurrentPopularity > PrevPopularity,
        "#009900",  -- Green when current popularity is greater
        CurrentPopularity < PrevPopularity,
        "#ff0000", -- Red
        CurrentPopularity = PrevPopularity,
        "#0000ff"   -- Blue for neutral cases (neither greater nor less)
    )

```


### Note: Detailed documentation in attached docx file under miscellaneous folder.
