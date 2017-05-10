import csv
import os
import psycopg2
from random import randint
from psycopg2 import connect 
from _sqlite3 import Row

fileNames = []
fileDirectories = []

#gets the names of all files in '(Current working directory)/GRADES' and stores the names/directory of each file in arrays
def getNames():
    cwd = os.getcwd()
    csvDir = cwd + '\Grades'
    files = os.listdir(csvDir)
    for file in files:
        if '.csv' in file:
            fileNames.append(file[:-4])
            fileDirectories.append(csvDir + "\\" + file)
   

#makes a table with named after each file, stick T in front because can't name tables starting with number
def makeTable(name, c):   
    temp = ('CREATE TABLE IF NOT EXISTS {}(CID text, TERM text, SUBJ text,' + 
            'CRSE text, SEC text, COURSEUNITS text, INSTRUCTORS text,' + 
            'TYPE text, DAYS text, TIME text, BUILD text,' +
            'ROOM text, SEAT text, SID text, SURNAME text,' +
            'PREFNAME text, LEVEL text, UNITS text, CLASS text, MAJOR text,' +
             'GRADE text, STATUS text, EMAIL text);')
    newName = "T" + name
    c.execute(temp.format(newName))


#reads files from the directory and inserts data into table
def insertData(table, directory, c):
    blank = ""
    tempStr = ""
    temp = open(directory, 'rt')
    data = csv.reader(temp)
    tempStr2 = []
    count = 0       #to keep track of tables inserted, 0 = beginning of file, 1 = table 1, 2 = table 2, 3 = table 3
    startFlag = 0   #to determine start of file
    skipFlag = 0    #to determine if need to skip line because data row empty
    justSkipped = 0 #to determine if just skipped a line because data row empty
    isList = 0      #to determine if tempStr2 is a list or a string
    for row in data:
        #print (count)
        if startFlag == 0:  #if file just started
            blank = row
            startFlag = 1
        if skipFlag == 1:   #if skip flag set, skip next line 
            skipFlag = 0
            justSkipped = 1
            continue
        if row == blank and justSkipped == 1: #to account for empty rows, if just skipped line, and current line is also empty, then the row contains no data
            #print("blank")
            if count == 1: #at CID table
                tempStr = ['','','','','','']
                justSkipped = 0
                count = count + 1
            if count == 2:  # at instructor table
                tempStr2 = ['','','','','','']
                justSkipped = 0
                isList = 0
                count = count + 1
            if count == 3:  # at student table
                tempList = ['','','','','','','','','','','']
                justSkipped = 0
                count = 0
                if isList == 1: #if tempStr2 is a list, join lists and insert into table
                    for j in range(0, len(tempStr2)):
                        temp2 = tempStr + tempStr2[j] + tempList
                        #print(temp2)
                        c.execute('INSERT INTO T'+ table + '(CID, TERM, SUBJ, CRSE, SEC, COURSEUNITS, INSTRUCTORS, TYPE, DAYS, TIME, BUILD, ROOM, SEAT,' +
                                  'SID, SURNAME, PREFNAME, LEVEL, UNITS, CLASS, MAJOR, GRADE, STATUS, EMAIL) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', 
                                  temp2)
                else:           #else tempStr2 is string do same but w/o list
                    temp2 = tempStr + tempStr2 + tempList
                    #print(temp2)
                    c.execute('INSERT INTO T'+ table + '(CID, TERM, SUBJ, CRSE, SEC, COURSEUNITS, INSTRUCTORS, TYPE, DAYS, TIME, BUILD, ROOM, SEAT,' +
                              'SID, SURNAME, PREFNAME, LEVEL, UNITS, CLASS, MAJOR, GRADE, STATUS, EMAIL) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', 
                              temp2)
                del tempStr2[:]
            #if count == 4:
                #justSkipped = 0
                #count = 0    
        if row != blank and skipFlag == 0:  #if row not empty
            if count == 1:
                tempStr = row
                justSkipped = 0
                #print (tempStr)
            if count == 2:
                tempStr2.append(row)
                isList = 1
                justSkipped = 0
                #print (tempStr2)
            if count == 3:
                justSkipped = 0
                if isList == 1:
                    for j in range(0, len(tempStr2)):
                        temp2 = tempStr + tempStr2[j] + row
                        #print(temp2)
                        c.execute('INSERT INTO T'+ table + '(CID, TERM, SUBJ, CRSE, SEC, COURSEUNITS, INSTRUCTORS, TYPE, DAYS, TIME, BUILD, ROOM, SEAT,' +
                                  'SID, SURNAME, PREFNAME, LEVEL, UNITS, CLASS, MAJOR, GRADE, STATUS, EMAIL) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', 
                                  temp2)
                else:
                    temp2 = tempStr + tempStr2 + row
                    #print(temp2)
                    c.execute('INSERT INTO T'+ table + '(CID, TERM, SUBJ, CRSE, SEC, COURSEUNITS, INSTRUCTORS, TYPE, DAYS, TIME, BUILD, ROOM, SEAT,' +
                              'SID, SURNAME, PREFNAME, LEVEL, UNITS, CLASS, MAJOR, GRADE, STATUS, EMAIL) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)', 
                              temp2)
        else:
            count = count + 1 #"" signifies start of new table so inc count
            if count == 4:    #tables are in groups of 3, so if count = 4, then must read new set of tables
                isList = 0    #reset list flag
                del tempStr2[:] #done with set of tables, so delete list/str
                count = 1     #reset count
            skipFlag = 1
    
            

if __name__ == '__main__':
    getNames()
    with connect('dbname= test user=postgres password = slay21er') as myCon:
        with myCon.cursor() as myCursor:
            makeTable("master",myCursor) #this table contains all information from all files
            for i in range(0, len(fileNames)):
                makeTable(fileNames[i], myCursor) #make tables for each file 
                myCon.commit()
                insertData(fileNames[i], fileDirectories[i], myCursor)
                name = "T" + fileNames[i]
                myCursor.execute("INSERT INTO tmaster SELECT * FROM " + name) 
                myCursor.execute("DROP TABLE " + name)
            #create STUDENT table
            myCursor.execute("CREATE TABLE IF NOT EXISTS STUDENT(SID text PRIMARY KEY, SURNAME text, PREFNAME text, EMAIL text);")
            myCursor.execute("INSERT INTO STUDENT SELECT DISTINCT SID, SURNAME, PREFNAME, EMAIL FROM tmaster WHERE SID != '' ;")
            #create STATUS table
            myCursor.execute("CREATE TABLE IF NOT EXISTS STATUS(SID text, TERM text, MAJOR text, CLASS text, LEVEL text, PRIMARY KEY(SID, TERM));")
            myCursor.execute("SELECT DISTINCT SID, TERM, MAJOR, CLASS, LEVEL FROM tmaster WHERE SID != '' ORDER BY SID, TERM")
            #rename duplicate keys 
            temp = myCursor.fetchall()
            SID = ""
            TERM = ""
            gotDupe = 0
            for i in range(0, len(temp)):
                if i == 0: #initial loop
                    SID = temp[i][0]
                    TERM = temp[i][1]
                    MAJOR = temp[i][2]
                    CLASS = temp[i][3]
                    LEVEL = temp[i][4]
                    continue
                if temp[i][0] == SID and temp[i][1] == TERM:  #if there is a duplicate, append -1 and -2 to duplicate strings and insert rows i, i-1 into table
                    SID = temp[i-1][0]
                    TERM = temp[i-1][1] + "-1"
                    MAJOR = temp[i-1][2]
                    CLASS = temp[i-1][3]
                    LEVEL = temp[i-1][4]
                    myCursor.execute("INSERT INTO STATUS(SID, TERM, MAJOR, CLASS, LEVEL) VALUES('" +
                                      str(SID) + "', '" + str(TERM) + "', '" + str(MAJOR) + "', '" + str(CLASS) + "', '" + str(LEVEL) + "');")
                    SID = temp[i ][0]
                    TERM = temp[i][1] + "-2"
                    MAJOR = temp[i][2]
                    CLASS = temp[i][3]
                    LEVEL = temp[i][4]
                    myCursor.execute("INSERT INTO STATUS(SID, TERM, MAJOR, CLASS, LEVEL) VALUES('" +
                                      str(SID) + "', '" + str(TERM) + "', '" + str(MAJOR) + "', '" + str(CLASS) + "', '" + str(LEVEL) + "');")
                    gotDupe = 1
                else:                                           #Row i not duplicate of row i-1, so insert row i-1 into table
                    if gotDupe == 0:
                        myCursor.execute("INSERT INTO STATUS(SID, TERM, MAJOR, CLASS, LEVEL) VALUES('" +
                                         str(SID) + "', '" + str(TERM) + "', '" + str(MAJOR) + "', '" + str(CLASS) + "', '" + str(LEVEL) + "');")
                    SID = temp[i][0]
                    TERM = temp[i][1]
                    MAJOR = temp[i][2]
                    CLASS = temp[i][3]
                    LEVEL = temp[i][4]
                    gotDupe = 0
                
                
            #create TAKE table
            myCursor.execute("CREATE TABLE IF NOT EXISTS TAKE(SID text, CID text, TERM text, " + 
                             "STUDENTUNITS text, GRADE text,STATUS text, SEAT text, PRIMARY KEY(SID, CID, TERM));")
            myCursor.execute("INSERT INTO TAKE SELECT DISTINCT SID, CID, TERM, UNITS, GRADE, STATUS, SEAT FROM tmaster WHERE SID != '';")
            #create COURSES Table
            myCursor.execute("CREATE TABLE IF NOT EXISTS COURSES(UNIT text, CID text, TERM text, SEC text, CRSE text, SUBJ text, PRIMARY KEY(CID, TERM));")
            myCursor.execute("SELECT DISTINCT COURSEUNITS, CID, TERM, SEC, CRSE, SUBJ FROM tmaster WHERE CID != '' ORDER BY CID, TERM ;")
            #rename duplicate keys
            temp = myCursor.fetchall()
            gotDupe = 0
            for i in range(0, len(temp)):
                if i == 0: #initial run
                    UNITS = temp[i][0]
                    CID = temp[i][1]
                    TERM = temp[i][2]
                    SECTION = temp[i][3]
                    COURSE = temp[i][4]
                    SUBJ = temp[i][5]
                    continue
                if temp[i][1] == CID and temp[i][2] == TERM:  #if there is a duplicate, append -1 and -2 to duplicate strings 
                    UNITS = temp[i-1][0]
                    CID = temp[i-1][1]
                    TERM = temp[i-1][2] + "-1"
                    SECTION = temp[i-1][3]
                    COURSE = temp[i-1][4]
                    SUBJ = temp[i-1][5]
                    myCursor.execute("INSERT INTO COURSES(UNIT, CID, TERM, SEC, CRSE, SUBJ) VALUES('" +
                                      str(UNITS) + "', '" + str(CID) + "', '" + str(TERM) + "', '" + str(SECTION) + "', '" + str(COURSE) + 
                                      "', '" + str(SUBJ) + "');")
                    UNITS = temp[i][0]
                    CID = temp[i][1]
                    TERM = temp[i][2] + "-2"
                    SECTION = temp[i][3]
                    COURSE = temp[i][4]
                    SUBJ = temp[i][5]
                    myCursor.execute("INSERT INTO COURSES(UNIT, CID, TERM, SEC, CRSE, SUBJ) VALUES('" +
                                      str(UNITS) + "', '" + str(CID) + "', '" + str(TERM) + "', '" + str(SECTION) + "', '" + str(COURSE) + 
                                      "', '" + str(SUBJ) + "');")
                    gotDupe = 1
                else:
                    if gotDupe == 0:
                        myCursor.execute("INSERT INTO COURSES(UNIT, CID, TERM, SEC, CRSE, SUBJ) VALUES('" +
                                      str(UNITS) + "', '" + str(CID) + "', '" + str(TERM) + "', '" + str(SECTION) + "', '" + str(COURSE) + 
                                      "', '" + str(SUBJ) + "');")
                    UNITS = temp[i][0]
                    CID = temp[i][1]
                    TERM = temp[i][2]
                    SECTION = temp[i][3]
                    COURSE = temp[i][4]
                    SUBJ = temp[i][5]
                    gotDupe = 0
        
            #create INSTRUCTOR table
            myCursor.execute("CREATE TABLE IF NOT EXISTS INSTRUCTOR(CID text, TERM text, INSTRUCTORS text, PRIMARY KEY(CID, TERM));")
            myCursor.execute("SELECT DISTINCT CID, TERM, INSTRUCTORS FROM tmaster WHERE CID != '' AND INSTRUCTORS != '' ORDER BY CID, TERM;")
            #rename duplicate strings
            temp = myCursor.fetchall()
            gotDupe = 0
            for i in range(0, len(temp)):
                if i == 0: #initial run
                    CID = temp[i][0]
                    TERM = temp[i][1]
                    INSTRUCTORS = temp[i][2]
                    if "O'" in INSTRUCTORS: #to account for O'whatever names
                        INSTRUCTORS = INSTRUCTORS[1:]
                        INSTRUCTORS = "O'" + INSTRUCTORS + "'"  
                if temp[i][0] == CID and temp[i][1] == TERM:  #if there is a duplicate, append -1 and -2 to duplicate strings 
                    CID = temp[i-1][0]
                    TERM = temp[i-1][1] + "-1"
                    INSTRUCTORS = temp[i-1][2]
                    if "O'" in INSTRUCTORS: #to account for O'whatever names
                        INSTRUCTORS = INSTRUCTORS[1:]
                        INSTRUCTORS = "O'" + INSTRUCTORS + "'" 
                        myCursor.execute("INSERT INTO INSTRUCTOR(CID, TERM, INSTRUCTORS) VALUES('" +
                                         str(CID) + "', '" + str(TERM) + "', '" + str(INSTRUCTORS) + ");")
                    else:
                        myCursor.execute("INSERT INTO INSTRUCTOR(CID, TERM, INSTRUCTORS) VALUES('" +
                                         str(CID) + "', '" + str(TERM) + "', '" + str(INSTRUCTORS) + "');")
                    CID = temp[i][0]
                    TERM = temp[i][1] + "-2"
                    INSTRUCTORS = temp[i][2]
                    if "O'" in INSTRUCTORS: #to account for O'whatever names
                        INSTRUCTORS = INSTRUCTORS[1:]
                        INSTRUCTORS = "O'" + INSTRUCTORS + "'" 
                        myCursor.execute("INSERT INTO INSTRUCTOR(CID, TERM, INSTRUCTORS) VALUES('" +
                                         str(CID) + "', '" + str(TERM) + "', '" + str(INSTRUCTORS) + ");")
                    else:
                        myCursor.execute("INSERT INTO INSTRUCTOR(CID, TERM, INSTRUCTORS) VALUES('" +
                                         str(CID) + "', '" + str(TERM) + "', '" + str(INSTRUCTORS) + "');")
                    gotDupe = 1
                else:
                    if gotDupe == 0:
                        if "O'" in INSTRUCTORS: #to account for O'whatever names
                            INSTRUCTORS = INSTRUCTORS[1:]
                            INSTRUCTORS = "O'" + INSTRUCTORS + "'" 
                            myCursor.execute("INSERT INTO INSTRUCTOR(CID, TERM, INSTRUCTORS) VALUES('" +
                                             str(CID) + "', '" + str(TERM) + "', '" + str(INSTRUCTORS) + ");")
                        else:
                            myCursor.execute("INSERT INTO INSTRUCTOR(CID, TERM, INSTRUCTORS) VALUES('" +
                                             str(CID) + "', '" + str(TERM) + "', '" + str(INSTRUCTORS) + "');")
                    CID = temp[i][0]
                    TERM = temp[i][1]
                    INSTRUCTORS = temp[i][2]
                    gotDupe = 0
        
            #create MEETINGS table
            myCursor.execute("CREATE TABLE IF NOT EXISTS MEETINGS(CID text, TERM text, TYPE text, DAYS text, " +
                             "TIME text, BUILDING text, ROOM text, ID int, PRIMARY KEY(CID, TERM, TYPE, DAYS, TIME, ID));")
            myCursor.execute("SELECT DISTINCT CID, TERM, TYPE, DAYS, TIME, BUILD, ROOM FROM tmaster;")
            temp = myCursor.fetchall()
            for i in range(0, len(temp)):
                random = randint(0, 10000)
                CID = temp[i][0]
                TERM = temp[i][1]
                TYPE = temp[i][2]
                DAYS = temp[i][3]
                TIME = temp[i][4]
                BUILDING = temp[i][5]
                ROOM = temp[i][6]
                myCursor.execute("INSERT INTO MEETINGS(CID, TERM, TYPE, DAYS, TIME, BUILDING, ROOM, ID) VALUES ('" + 
                                 CID + "','" + TERM + "','" + TYPE + "','" + DAYS + "','" + TIME +"','" + BUILDING + "','" + ROOM + "'," + str(random) + ");")
            #myCursor.execute("DROP TABLE tmaster;")
            
            
            
