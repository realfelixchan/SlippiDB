#!/usr/bin/env python
# coding: utf-8

# In[ ]:


## Requires nodeenv to run node programs within the venv
import os
import subprocess
import io
import json
from contextlib import redirect_stdout
import glob
import slippi
from slippi.parse import ParseEvent, ParseError
import re
import pandas as pd
import numpy as np
import pyparsing as parse
import datetime
import duckdb
import boto3
import psycopg2
import time


# In[ ]:


# Takes a dictionary of parseEvents and handlers and game file destination and returns a Dictionary of strings
# Dependencies: parseDict, io, contextlib, slippi.parse
# Input: Slippi File path, dictionary of event handlers from slippi.parse
# Output: Dictionary {fileName, fileMetadata}
def parseFile(slippiFile, parseDict):
    parsedDict = {}
    parseDict['gameID'] = str(slippiFile)
    with io.StringIO() as buffer, redirect_stdout(buffer): 
        for key in parseDict:
            slippi.parse(slippiFile, {key:print})
            parsedDict[str(key).lower().replace('parseevent.', '')] = buffer.getvalue()
    return parsedDict


# In[ ]:


# Iterate for slippi files
# Dependencies: glob
# Input: Directory path
# Output: glob iterator to track each .slp file
def getSlippiFiles(baseDir):
    return glob.glob(baseDir+"/**/*.slp", recursive=True)


# In[ ]:


# Return the metadata for the JS package
def getJSMetadata(slippiFile):
    jsMetadataDict = {}
    try:
        output=subprocess.check_output(r'node .\slippiStats.js '+slippiFile, shell=True) 
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
        # This has to do with the way the JS library parses the data, will just skip 
    jsMetadataDict[str(slippiFile)] = str(output).lstrip('b')
    return jsMetadataDict


# In[ ]:


# Takes any data as a list and splits it evenly between odd indices and even indices
# Created function because of how data needs to be split with the way it's parsed
# Input: A list
# Output: The inputted list split into two lists
def splitList(dataAsList):
    playerOneList = dataAsList[0::2]
    playerTwoList = dataAsList[1::2]
    return playerOneList, playerTwoList


# In[ ]:


# Takes the JSmetadata information as a String and return player names, both Player 1 and Player 2 from String value, as two separaate Lists
# Dependencies: re, splitList
# Input: Metadata String from getJSMetadata
# Output: Two lists that track the player names for each player per game file
def getPlayerNames(metadataString):
    playerNames = re.findall("displayName: (.*?),", metadataString)
    playerNames = [name.strip(r'\\\'') for name in playerNames]
    playerOneList, playerTwoList = splitList(playerNames)
    return playerOneList, playerTwoList


# In[ ]:


# Takes the metadata information as a String and return character names, both Plyaer 1 and Player 2, as two separate lists
# Dependencies: re, splitList
# Input: Metadata String from getJSMetadata
# Output: Two lists that track the character ID for each player per game file
def getcharIds(metadataString):
    charIds = re.findall(r'(?:characterId: )(\d+)', metadataString)
    playerOneList, playerTwoList = splitList(charIds)
    return playerOneList, playerTwoList


# In[ ]:


# Takes metadata information as a String and returns the stocks taken by each player as two separate lists
# Dependencies: pyparsing, splitList
# Input: Metadata String that is created from getJSMetadata
# Output: Two lists that track stocks taken for each character per game file
def getStocksTaken(metadataString):
    stocksTaken = []
    killCount = parse.Literal('killCount:').suppress() + parse.Word(parse.nums)
    for match in killCount.scanString(metadataString):
        stocksTaken.append(match[0][0])
    playerOneList, playerTwoList = splitList(stocksTaken)
    return playerOneList, playerTwoList


# In[ ]:


# Takes a directory path that contains all Slippi files to analyze and place into a dictionary
# Dependencies: parseData, slippi.parse
# Input: Directory path
# Output: Dictionary {fileName, fileMetadata}
def parsedData(slippiFile):
    handlers = {ParseEvent.FRAME: print, ParseEvent.METADATA: print}
    # Dicts where the key is the name of the file, and the value is the string value output
    metadataDict = {}
    # Came across an issue with the raw data in some select files
    try:
        parseData = parseFile(slippiFile, handlers)
    except (ValueError, ParseError) as error:
        print('The file has an issue with its raw data, moving on.')
        # This is an issue with the way the data is parsed by the Python library
        return
    metadataDict[str(slippiFile)] = parseData['metadata']
    return metadataDict


# In[ ]:


# Only processes files that haven't been previously inserted into the database.
# This is done by having duckDB's database file to insert into, as well as a log file for checking time stamp
# Input: Directory path
# Output: via processSlippiFiles, a CSV
def processNewFiles(slippiPath):
# Get the last processed timestamp from the log file
    os.chdir(slippiPath)
    slippiFiles = getSlippiFiles(slippiPath)
    print(slippiFiles)
    try:
        with open(logFile, 'r') as f:
            lastProcessed = float(f.read())
    except FileNotFoundError:
        lastProcessed = 0
    newFiles = [f for f in slippiFiles if os.path.isfile(f) and os.path.getmtime(f) > lastProcessed]
    newFiles.sort()
    processSlippiFiles(newFiles)


# In[ ]:


# Takes a directory and parses through the raw data of each file. The purpose is to create CSV files for
# uploading to AWS tables
# Dependent on the above parsing functions, re, pandas, duckDB, processNewFiles
# Input: File directory of .slp files
# Output: Main CSV
def processSlippiFiles(slippiPath):
    conn = duckdb.connect(database='playerDB.db', read_only=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS gamedata (GameID varchar(25), 
    PlayerOneName varchar(16),
    PlayerTwoName varchar(16),
    PlayerOneCharacterID integer,
    PlayerTwoCharacterID integer,
    PlayerOneStocksTaken integer, 
    PlayerTwoStocksTaken integer, 
    StageID integer,
    GameDate varchar(30), 
    GameLength varchar(6))""")
    
    for file in slippiPath:
        # Data parsing
        metadataDict = parsedData(file)
        jsMetadataDict = getJSMetadata(file)
        playerOneNames, playerTwoNames = getPlayerNames(str(jsMetadataDict.values()))
        playerOneChars, playerTwoChars = getcharIds(str(jsMetadataDict.values()))
        playerOneStocksTaken, playerTwoStocksTaken = getStocksTaken(str(jsMetadataDict.values()))
        stageIds = re.findall(r'(?:stageId: )(\d+)', str(jsMetadataDict.values()))
        gameIDs = [i.lstrip(slippiPath) for i in list(jsMetadataDict.keys())]
        gameIDs = [i.rstrip('.slp') for i in gameIDs]
        gameDates = re.findall(r'(?:startAt: )(\S*)\'', str(jsMetadataDict.values()))
        gameDates = [i.strip('\\') for i in gameDates]
        gameDates = [i.lstrip('\'') for i in gameDates]
        gameLengths = re.findall(r'(?:lastFrame: )(\d+)', str(jsMetadataDict.values()))
        
        # String is duplicated a second time, so we're only taking every other result
        gameLengths = gameLengths[0::2]
        gameLengths = [round(int(i)/3600, 3) for i in gameLengths]
        
        #Loading the data into a temporary dictionary -> temporary dataframe -> Insert to table
        gameData = {'GameID': gameIDs, 'PlayerOneName': playerOneNames, 'PlayerTwoName': playerTwoNames,
                    'PlayerOneCharacterID': playerOneChars, 'PlayerTwoCharacterID': playerTwoChars,
                    'PlayerOneStocksTaken': playerOneStocksTaken, 'PlayerTwoStocksTaken': playerTwoStocksTaken,
                    'StageId': stageIds, 'GameDate': gameDates, 'GameLength': gameLengths}

        gameDataDF = pd.DataFrame(data=gameData)
        with open(logFile, 'w') as f:
            f.write(str(time.time()))
        conn.execute("INSERT INTO GameData SELECT * FROM gameDataDF")

    conn.execute("COPY gameData TO 'GameData.csv' (HEADER, DELIMITER ',')")


# In[ ]:


# Creating derivative CSVs for other tables
# Dependencies: Pandas, numpy 
# Input: CSV created by processSLippiFiles, string of player name
# Output: Two CSVs
def createOtherCSVs(name = '2Chans'):
    gameDataDF=pd.read_csv('.\GameData.csv')
    conditions = [
        (gameDataDF['PlayerOneStocksTaken'] == '4') | (gameDataDF['PlayerOneStocksTaken'] > gameDataDF['PlayerTwoStocksTaken']),
        (gameDataDF['PlayerTwoStocksTaken'] == '4') | (gameDataDF['PlayerOneStocksTaken'] < gameDataDF['PlayerTwoStocksTaken']),
        (gameDataDF['PlayerOneStocksTaken'] == gameDataDF['PlayerTwoStocksTaken'])]
    choices = ['1', '2', '0']
    gameDataDF['Winner'] = np.select(conditions, choices, default='0')

    selfPlayerOne=gameDataDF.loc[gameDataDF['PlayerOneName'] == name][['PlayerOneCharacterID', 'PlayerTwoCharacterID', 'StageID', 'Winner']]
    # Flipping the order of the character IDs to make later concatenation more intuitive
    selfPlayerTwo=gameDataDF.loc[gameDataDF['PlayerTwoName'] == name][['PlayerTwoCharacterID', 'PlayerOneCharacterID', 'StageID', 'Winner']]
    
    # Changing column names and winner values to be more consistent
    selfPlayerOne.rename(columns={'PlayerOneCharacterID':'SelfCharacterID',
                                 'PlayerTwoCharacterID':'OtherCharacterID'}, inplace=True)
    conditions = [
        (selfPlayerOne['Winner']=='1'),
        (selfPlayerOne['Winner']=='2'),
        (selfPlayerOne['Winner']=='0')
    ]
    choices = ['Self', 'Other', 'None']
    selfPlayerOne['Winner'] = np.select(conditions, choices, default='None')

    selfPlayerTwo.rename(columns={'PlayerTwoCharacterID':'SelfCharacterID',
                                 'PlayerOneCharacterID':'OtherCharacterID'}, inplace=True)

    conditions = [
        (selfPlayerTwo['Winner']=='2'),
        (selfPlayerTwo['Winner']=='1'),
        (selfPlayerTwo['Winner']=='0')
    ]
    choices = ['Self', 'Other', 'None']
    selfPlayerTwo['Winner'] = np.select(conditions, choices, default='None')

    # Group matchup data by characters played and faced, return a win percentage (not including draws/unknown)
    matchupResults=pd.concat([selfPlayerOne,selfPlayerTwo]).groupby(['SelfCharacterID', 'OtherCharacterID'])['Winner']\
               .apply(lambda x: 100*((x == 'Self').sum() / (x.isin(['Self', 'Other']).sum()) or np.nan))\
               .reset_index(name='Percentage')

    stageResults=pd.concat([selfPlayerOne,selfPlayerTwo]).groupby(['StageID'])['Winner']\
               .apply(lambda x: 100*((x == 'Self').sum() / (x.isin(['Self', 'Other']).sum()) or np.nan))\
               .reset_index(name='Percentage')

    ## Matchup table csv
    matchupResults.to_csv('MatchupResults.csv')
    ## Stage table csv
    stageResults.to_csv('StageResults.csv')


# In[ ]:


# Define function to process CSV and upload to RDS
# Depndencies: boto3, psycopg2, createOtherCSVs, processNewFiles
# Input: String of CSV path, String of destination table
# Output: Table insert/update in RDS
def upload_to_rds(csvPath, tableName):
   # create an STS client and get temporary credentials for the IAM role
    sts = boto3.client('sts')
    response = sts.assume_role(
        RoleArn=roleARN,
        RoleSessionName='slippi'
    )
    credentials = response['Credentials']

    # create a connection to the RDS instance using psycopg2
    conn = psycopg2.connect(
        host='slippi-database.cdkpriuzfx7h.us-east-1.rds.amazonaws.com',
        port=5432,
        dbname='slippi-database',
        user= rdsUsername,
        password= rdsPassword,
        sslmode='require'
    )

    # create a cursor and execute the COPY command to upload the CSV data
    cur = conn.cursor()
    cur.execute("""
        COPY '{}'
        FROM STDIN
        IAM_ROLE '{}'
        CSV HEADER;
    """.format(tableName, os.environ['IAM_ROLE_ARN']))

    # open the CSV file and copy its contents to the database
    with open(csvPath, 'r') as f:
        cur.copy_expert(sql=f"CSV", file=f)

    conn.commit()

    # close the cursor and connection
    cur.close()
    conn.close()


# In[ ]:


slippiPath = r"C:\Users\pleasework\Documents\Slippi\SlippiAnalysisFiles"
logFile = ".\logFile.txt"


# In[ ]:


roleARN = os.environ.get('slippiRoleARN')
rdsUsername = os.environ['slippiRDSUsername']
rdsPassword = os.environ['slippiRDSPassword']


# In[ ]:


processNewFiles(slippiPath)
createOtherCSVs('2Chans')


# In[ ]:


upload_to_rds('.\GameData.csv', 'gamedata')
upload_to_rds('.\MatchupResults.csv', 'matchupresults')
upload_to_rds('.\StageResults.csv', 'stageresults')

