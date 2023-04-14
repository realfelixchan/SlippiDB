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
import matplotlib.pyplot as plt
import duckdb
get_ipython().run_line_magic('matplotlib', 'inline')


# Takes a dictionary of parseEvents and handlers and game file destination
# Returns a Dictionary of strings
def parseFile(slippiFile, parseDict):
    parsedDict = {}
    parseDict['gameID'] = str(slippiFile)
    with io.StringIO() as buffer, redirect_stdout(buffer): 
        for key in parseDict:
            slippi.parse(slippiFile, {key:print})
            parsedDict[str(key).lower().replace('parseevent.', '')] = buffer.getvalue()
    return parsedDict


# Iterate for slippi files
def getSlippiFiles(baseDir):
    return glob.glob(baseDir+"/**/*.slp", recursive=True)


# Return the metadata for the JS package
def getJSMetadata(slippiFile):
    jsMetadataDict = {}
    try:
        output=subprocess.check_output(r'node .\slippiStats.js '+slippiFile, shell=True) 
    except subprocess.CalledProcessError as e:
        raise RuntimeError("command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output))
        # This has to do with the way the JS library parses the data, will just skip 
        return
    jsMetadataDict[str(file)] = str(output).lstrip('b')
    return jsMetadataDict


# Takes any data as a list and splits it evenly between odd indices and even indices
# Returns the two halves as two separate lists
# Created function because of how data needs to be split with the way it's parsed
def splitList(dataAsList):
    playerOneList = dataAsList[0::2]
    playerTwoList = dataAsList[1::2]
    return playerOneList, playerTwoList


# Takes the JSmetadata information as a String
# Return player names, both Player 1 and Player 2 from String value, as two separaate Lists
def getPlayerNames(metadataString):
    playerNames = re.findall("displayName: (.*?),", metadataString)
    playerNames = [name.strip(r'\\\'') for name in playerNames]
    playerOneList, playerTwoList = splitList(playerNames)
    return playerOneList, playerTwoList


# Takes the metadata information as a String
# Return character names, both Plyaer 1 and Player 2, as two separate lists
def getcharIds(metadataString):
    charIds = re.findall(r'(?:characterId: )(\d+)', metadataString)
    playerOneList, playerTwoList = splitList(charIds)
    return playerOneList, playerTwoList


# Takes metadata information as a String 
# Returns the stocks taken by each player as two separate lists
def getStocksTaken(metadataString):
    stocksTaken = []
    killCount = parse.Literal('killCount:').suppress() + parse.Word(parse.nums)
    for match in killCount.scanString(metadataString):
        stocksTaken.append(match[0][0])
    playerOneList, playerTwoList = splitList(stocksTaken)
    return playerOneList, playerTwoList


# Takes a directory path that contains all Slippi files to analyze
# Returns parsed data dictionary
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
    metadataDict[str(file)] = parseData['metadata']
    return metadataDict


slippiPath = r"C:\Users\pleasework\Documents\Slippi"


# conn = duckdb.connect(database='playerDB.db', read_only=False)
conn = duckdb.connect()
conn.execute("""CREATE TABLE GameData (GameID varchar(25), 
PlayerOneName varchar(16),
PlayerTwoName varchar(16),
PlayerOneCharacterID integer,
PlayerTwoCharacterID integer,
PlayerOneStocksTaken integer, 
PlayerTwoStocksTaken integer, StageID integer,
GameDate varchar(30), GameLength varchar(6))""")


slippiFiles = getSlippiFiles(slippiPath)
for file in slippiFiles:
   print(file)
   metadataDict = parsedData(file)
   jsMetadataDict = getJSMetadata(file)
   playerOneNames, playerTwoNames = getPlayerNames(str(jsMetadataDict.values()))
   playerOneChars, playerTwoChars = getcharIds(str(jsMetadataDict.values()))
   playerOneStocksTaken, playerTwoStocksTaken = getStocksTaken(str(jsMetadataDict.values()))\
   stageIds = re.findall(r\'(?:stageId: )(\\d+)\', str(jsMetadataDict.values())
   gameIDs = [i.lstrip(slippiPath) for i in list(jsMetadataDict.keys())]
   gameIDs = [i.rstrip(\'.slp\') for i in gameIDs]
   gameDates = re.findall(r\'(?:startAt: )(\\S*)\\\'\', str(jsMetadataDict.values()))
   gameDates = [i.strip(\'\\\\\') for i in gameDates]
   gameDates = [i.lstrip(\'\\\'\') for i in gameDates]
   gameLengths = re.findall(r\'(?:lastFrame: )(\\d+)\', str(jsMetadataDict.values()))
   # String is duplicated a second time, so we\'re only taking every other result
   gameLengths = gameLengths[0::2]
   gameLengths = [round(int(i)/3600, 3) for i in gameLengths]\
   gameData = {\'GameID\': gameIDs, \'PlayerOneName\': playerOneNames, \'PlayerTwoName\': playerTwoNames,
   \'PlayerOneCharacterID\': playerOneChars, \'PlayerTwoCharacterID\': playerTwoChars,
   \'PlayerOneStocksTaken\': playerOneStocksTaken, \'PlayerTwoStocksTaken\': playerTwoStocksTaken,
   \'StageId\': stageIds, \'GameDate\': gameDates, \'GameLength\': gameLengths}
   gameDataDF = pd.DataFrame(data=gameData)
   conn.execute("INSERT INTO GameData SELECT * FROM gameDataDF")
   conn.execute("COPY gameData TO \'GameData.csv\' (HEADER, DELIMITER \',\')")\n')