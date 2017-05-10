import csv
import os
import psycopg2
from psycopg2 import connect 
from _sqlite3 import Row

tableNames = []

#gets the names of all files in '(Current working directory)/GRADES' and stores the names/directory of each file in arrays
def getNames():
    cwd = os.getcwd()
    csvDir = cwd + '/Grades'
    files = os.listdir(csvDir)
    for file in files:
        if '.csv' in file:
            temp = "T" + file
            tableNames.append(temp[:-4])

def calcUnits(grade, units):
    if grade == "I" or grade == "IP" or grade == "NG" or grade == "P" or grade == "NP" or grade == "Y" or grade == "U" or grade == "XR" or grade == "NS" or grade == "S":
        return(0)
    else: 
        return(float(units))

def convertGrade(grade):
    if grade == "A" or grade == "A+":
        return(float(4))
    if grade == "A-":
        return(float(3.7))
    if grade == "B+":
        return(float(3.3))
    if grade == "B":
        return(float(3))
    if grade == "B-":
        return(float(2.7))
    if grade == "C+":
        return(float(2.3))
    if grade == "C":
        return(float(2))
    if grade == "C-":
        return(float(1.7))
    if grade == "D+":
        return(float(1.3))
    if grade == "D":
        return(float(1))
    if grade == "D-":
        return(float(0.7))
    else:
        return(0)

def scoreGrade(grade):
    if grade == "A" or grade == "A+":
        return(float(4))
    if grade == "A-":
        return(float(3.7))
    if grade == "B+":
        return(float(3.3))
    if grade == "B":
        return(float(3))
    if grade == "B-":
        return(float(2.7))
    if grade == "C+":
        return(float(2.3))
    if grade == "C":
        return(float(2))
    if grade == "C-":
        return(float(1.7))
    if grade == "D+":
        return(float(1.3))
    if grade == "D":
        return(float(1))
    if grade == "D-":
        return(float(0.7))
    if grade == "F":
        return(float(0))
    else:
        return(-1)     #grade not applicable for scoring       

def partA(c):     #calculate % of students who took 1-20 units of ABC or DEF courses
    count = 0
    results = open("resultsPartA.txt", "a")
    results.write("---------------------------------------------------------------------------------\n")
    c.execute("Select COUNT(SID),S From (Select SUM(Cast(STUDENTUNITS as float)) AS S, term, SID from take WHERE STUDENTUNITS != '' group by term, SID) T group by T.S order by T.S;")
    temp = c.fetchall()
    integer = []
    for i in range(0,len(temp)):
        if float(temp[i][1]) in range(1,21):
            integer.append(temp[i])
    for i in range(0,len(temp)):
        count = count + float(temp[i][0])
    for i in range(0,len(integer)):
        results.write(str(integer[i][1]) + " units: ")
        results.write(str(100*float(integer[i][0])/count) + " %\n")
    results.write("---------------------------------------------------------------------------------\n")






def partB(c): #calculate average gpa of students that took 1-20 units of ABC or DEF courses
    results = open("resultsPartB.txt", "a")
    results.write("---------------------------------------------------------------------------------\n")
    #get gradepoints and units completed of each student taking ABC
    c.execute("SELECT SID, CID, Take.Term AS Term, STUDENTUNITS, GRADE FROM Take WHERE STUDENTUNITS != '' GROUP BY SID,STUDENTUNITS,GRADE,Take.Term,CID")
    temp = c.fetchall()
    c.execute("CREATE TABLE IF NOT EXISTS GR(SID text, CID text, Term text, Units float, Grade float);")
    for i in range(0,len(temp)):
        calunit = calcUnits(temp[i][4],temp[i][3])
        calgrade = scoreGrade(temp[i][4])
        c.execute("INSERT INTO GR(SID,CID,Term, Units,Grade) VALUES('" + str(temp[i][0]) + "','" + str(temp[i][1]) + "','" + str(temp[i][2]) + "','" + str(calunit) + "','" + str(calgrade) + "');")
    c.execute("SELECT AVG(A) AS stuavg, S From(SELECT Sum(Units*Grade)/Sum(Units) AS A,Sum(Units) AS S, Term,SID FROM GR WHERE Grade >= 0 GROUP BY Term,SID HAVING Sum(Cast(Units as float)) BETWEEN 1 AND 20) T Group by T.S order by T.S;")
    temp = c.fetchall()
    for i in range(0,len(temp)):
        results.write(str(temp[i][1]) + " units: ")
        results.write(str(temp[i][0]) + "\n")
    results.write("---------------------------------------------------------------------------------\n")
    c.execute("DROP TABLE GR;")





def partC(c): #determine easiest and hardest instructor based on grades
    results = open("resultsPartC.txt", "w")
    c.execute("SELECT INSTRUCTORS, SID, GRADE, STUDENTUNITS FROM INSTRUCTOR, Take WHERE STUDENTUNITS != ' ' AND Take.CID = INSTRUCTOR.CID AND Take.Term = INSTRUCTOR.Term ORDER BY INSTRUCTORS")
    temp = c.fetchall()
    c.execute("CREATE TABLE IF NOT EXISTS INS(INSTRUCTORS text, SID text, Grade float,Units float);")
    for i in range(0,len(temp)):
        calgrade = scoreGrade(temp[i][2])
        calunit = calcUnits(temp[i][2],temp[i][3])
        name = temp[i][0]
        if "O'" in name:
            name = name[1:]
            name = "O'" + name + "'"
            c.execute("INSERT INTO INS(INSTRUCTORS,SID,Grade,Units) VALUES('" + str(name) + ",'" + str(temp[i][1]) + "','" + str(calgrade) + "','" + str(calunit) + "');")
        else:
            c.execute("INSERT INTO INS(INSTRUCTORS,SID,Grade,Units) VALUES('" + str(name) + "','" + str(temp[i][1]) + "','" + str(calgrade) + "','" + str(calunit) + "');")
    c.execute("SELECT instructors,Sum(Units*grade)/Sum(units) AS A FROM INS WHERE GRADE >= 0 GROUP BY INSTRUCTORS EXCEPT SELECT T1.instructors AS instructor,T1.A AS A FROM (SELECT Sum(Units*grade)/Sum(units) AS A, INSTRUCTORS FROM INS WHERE GRADE >= 0 GROUP BY INSTRUCTORS) T1, (SELECT Sum(Units*grade)/Sum(units) AS A, INSTRUCTORS FROM INS WHERE GRADE >= 0 GROUP BY INSTRUCTORS) T2 WHERE T1.A < T2.A;")
    temp = c.fetchall()
    results.write("---------------------------------------------------------------------------------\n")
    results.write("The Easiest Instructor(with the highest assigned GPA) :\n")
    for i in range(0,len(temp)):
        results.write(str(temp[i][0]) + " : ")
        results.write(str(temp[i][1]) + "\n")
    c.execute("SELECT instructors,Sum(Units*grade)/Sum(units) AS A FROM INS WHERE GRADE >= 0 GROUP BY INSTRUCTORS EXCEPT SELECT T2.instructors AS instructor,T2.A AS A FROM (SELECT Sum(Units*grade)/Sum(units) AS A, INSTRUCTORS FROM INS WHERE GRADE >= 0 GROUP BY INSTRUCTORS) T1, (SELECT Sum(Units*grade)/Sum(units) AS A, INSTRUCTORS FROM INS WHERE GRADE >= 0 GROUP BY INSTRUCTORS) T2 WHERE T1.A < T2.A;")
    temp = c.fetchall()
    results.write("\n")
    results.write("---------------------------------------------------------------------------------\n")
    results.write("The Hardest Instructor(with the lowest assigned GPA) :\n")
    for i in range(0,len(temp)):
        results.write(str(temp[i][0]) + " : ")
        results.write(str(temp[i][1]) + "\n")
    c.execute("DROP TABLE INS;")



def partD(c):
    results = open("resultsPartD.txt", "w")
    c.execute("SELECT crse,subj,instructors,studentunits,grade,T.cid,T.term from (SELECT CRSE, SUBJ, instructors,Courses.CID,Courses.Term FROM Courses,instructor WHERE SUBJ = 'ABC' AND Courses.CID = instructor.CID AND Courses.Term = instructor.Term AND CRSE >= '100' AND CRSE < '200' Group by CRSE,SUBJ,instructors,Courses.CID,Courses.Term ORDER BY CRSE) T, Take where T.CID = Take.CID and T.term = Take.term group by crse,subj,instructors,studentunits,grade,T.cid,T.term;")
    temp = c.fetchall()
    c.execute("CREATE TABLE IF NOT EXISTS ABC1(crse text, subj text, instructor text, Units float,Grade float, CID text, Term text);")
    for i in range(0,len(temp)):
        calunit = calcUnits(temp[i][4],temp[i][3])
        calgrade = scoreGrade(temp[i][4])
        name = temp[i][2]
        if "O'" in name:
            name = name[1:]
            name = "O'" + name + "'"
            c.execute("INSERT INTO ABC1(crse,subj,instructor,Units,grade,cid,term) VALUES('" + str(temp[i][0]) + "','" + str(temp[i][1]) +  "','" +str(name) + ",'" + str(calunit) + "','" + str(calgrade) + "','" + str(temp[i][5]) + "','" + str(temp[i][6]) + "');")
        else:
            c.execute("INSERT INTO ABC1(crse,subj,instructor,Units,grade,cid,term) VALUES('" + str(temp[i][0]) + "','" + str(temp[i][1]) +  "','" + str(name) + "','" + str(calunit) + "','" + str(calgrade) + "','" + str(temp[i][5]) + "','" + str(temp[i][6]) + "');")
    results.write("For the Letter Grade:\n")
    results.write("\n")
    c.execute("select subj, crse, instructor, a from ((select cid,subj,crse,instructor,sum(units*grade)/sum(units) as A from abc1 where grade>=0 group by crse,subj,instructor,cid,term order by crse) except (select T2.cid,T2.subj,T2.crse,T2.instructor,T2.a from (select crse,subj,instructor,cid,term,sum(units*grade)/sum(units) as A  from abc1 where grade >=0 group by crse,subj,instructor,cid,term order by crse)T1,(select crse,subj,instructor,cid,term,sum(units*grade)/sum(units) as A from abc1 where grade >=0 group by crse,subj,instructor,cid,term order by crse)T2 where T1.crse = T2.crse and T1.a < T2.a)) P order by crse;")
    temp1 = c.fetchall()
    for i in range(len(temp1)):
        while ((i < (len(temp1) - 1)) and (temp1[i] == temp1[i+1])):
            del temp1[i]
    c.execute("select subj, crse, instructor, a from ((select cid,subj,crse,instructor,sum(units*grade)/sum(units) as A from abc1 where grade>=0 group by crse,subj,instructor,cid,term order by crse) except (select T1.cid,T1.subj,T1.crse,T1.instructor,T1.a from (select crse,subj,instructor,cid,term,sum(units*grade)/sum(units) as A  from abc1 where grade >=0 group by crse,subj,instructor,cid,term order by crse)T1,(select crse,subj,instructor,cid,term,sum(units*grade)/sum(units) as A from abc1 where grade >=0 group by crse,subj,instructor,cid,term order by crse)T2 where T1.crse = T2.crse and T1.a < T2.a)) P order by crse;")
    temp2 = c.fetchall()
    for i in range(len(temp2)):
        while ((i < (len(temp2) - 1)) and (temp2[i] == temp2[i+1])):
            del temp2[i]
    for i in range(0,len(temp1)):
        results.write("The hardest instructor: " + str(temp1[i][1]) + " " + str(temp1[i][0]) + " " + str(temp1[i][2]) + " : " + str(temp1[i][3]) + "\n")
    results.write("---------------------------------------------------------------------------------\n")
    for i in range(0,len(temp2)):
        results.write("The easiest instructor: " + str(temp2[i][1]) + " " + str(temp2[i][0]) + " " + str(temp2[i][2]) + " : " + str(temp2[i][3]) + "\n")
    results.write("---------------------------------------------------------------------------------\n")
    results.write("For the P/NP courses:\n")
    results.write("\n")
    results.write("ABC 112: \n")
    results.write("The pass rate:\n")
    results.write("\n")
    c.execute("select T.grade,T.subj,T.crse,instructor.instructors,count(grade) as c from (select take.cid,sid,take.term,grade,studentunits,subj,crse from take,courses where take.cid = courses.cid and take.term = courses.term and subj = 'ABC' and crse = '112' and grade != ' ' group by crse,take.cid,sid,take.term,grade,studentunits,subj) T, instructor where T.cid = instructor.cid and T.term = instructor.term group by T.grade,T.subj,T.crse,instructor.instructors;")
    temp = c.fetchall()
    for i in range(0,len(temp)):
        results.write(str(temp[i][3]) + ":" + "100%\n")
    results.write("---------------------------------------------------------------------------------\n")
    results.write("ABC 113: \n")
    results.write("The pass rate:\n")
    results.write("\n")
    c.execute("select T.grade,T.subj,T.crse,instructor.instructors,count(grade) as c from (select take.cid,sid,take.term,grade,studentunits,subj,crse from take,courses where take.cid = courses.cid and take.term = courses.term and subj = 'ABC' and crse = '113' and grade != ' ' group by crse,take.cid,sid,take.term,grade,studentunits,subj) T, instructor where T.cid = instructor.cid and T.term = instructor.term group by T.grade,T.subj,T.crse,instructor.instructors;")
    temp = c.fetchall()
    results.write("The lowest Pass rate:" + str(temp[0][3]) + " : " + str(100*(float(temp[1][4])/(float(temp[1][4]) + float(temp[0][4])))) + "%\n")
    results.write("\n")
    for i in range(2,len(temp)):
        results.write(str(temp[i][3]) + ":" + "100%\n")
    results.write("---------------------------------------------------------------------------------\n")
    results.write("ABC 114: \n")
    results.write("The pass rate:\n")
    c.execute("select T.grade,T.subj,T.crse,instructor.instructors,count(grade) as c from (select take.cid,sid,take.term,grade,studentunits,subj,crse from take,courses where take.cid = courses.cid and take.term = courses.term and subj = 'ABC' and crse = '113' and grade != ' ' group by crse,take.cid,sid,take.term,grade,studentunits,subj) T, instructor where T.cid = instructor.cid and T.term = instructor.term group by T.grade,T.subj,T.crse,instructor.instructors;")
    temp = c.fetchall()
    for i in range(0,len(temp)):
        results.write(str(temp[i][3]) + ":" + "100%\n")
    c.execute("DROP TABLE abc1;")







def partE(c):
    results = open("resultsPartE.txt", "w")
    c.execute("select meetings.cid,meetings.term,days,time,building,room,crse,subj from meetings,courses where days !=' ' and time != ' ' and building != ' ' and room != ' ' and meetings.cid = courses.cid and meetings.term = courses.term order by term;")
    temp = c.fetchall()
    c.execute("create table if not exists summer(cid text, term text, days text, start text, close text, building text, room text,crse text,subj text);")
    arrange = []
    for i in range(0,len(temp)):
        if '06' in temp[i][1][4:]:
            arrange.append(temp[i])
    for i in range(0,len(arrange)):
        time = arrange[i][3]
        time = time.split(' - ')
        e = []
        for j in range(len(time)):
            if 'AM' in time[j]:
                e.append(time[j].split(' ')[0].split(':')[0] + time[j].split(' ')[0].split(':')[1])
            if 'PM' in time[j]:
                if '12' in time[j]:
                    e.append(time[j].split(' ')[0].split(':')[0] + time[j].split(' ')[0].split(':')[1])
                else:
                    e.append(str(int(time[j].split(' ')[0][0]) + 12) + time[j].split(' ')[0][2:4])
            if len(e) == 2:
                c.execute("insert into summer(cid,term,days,start,close,building,room,crse,subj) values('" + str(arrange[i][0]) + "','" + str(arrange[i][1]) + "','" + str(arrange[i][2]) + "','"+ str(e[0])+ "','"+ str(e[1]) + "','"+ str(arrange[i][4]) + "','"+ str(arrange[i][5]) + "','"+ str(arrange[i][6]) +  "','"+ str(arrange[i][7]) + "');")
    c.execute("select distinct T1term,T1cid,T1days,T1start,T1close,T1subj,T1crse,T.cid,days,start,close,subj,crse,building,room from ((select distinct T1.term as T1term,T1.cid as T1cid,T1.days as T1days,T1.start as T1start,T1.close as T1close,T1.subj as T1subj, T1.crse as T1crse,T2.term,T2.cid,T2.days,T2.start,T2.close,T2.subj,T2.crse,T2.building,T2.room from (select * from summer)T1,(select * from summer)T2 where T1.building = T2.building and T1.room = T2.room and T1.term = T2.term and T1.cid != T2.cid and (T1.subj != T2.subj or T1.crse != T2.crse) order by T1.term,T1.cid,T2.cid,T1.days,T2.days,T1.start,T2.start) except (select distinct T1.term as T1term,T1.cid as T1cid,T1.days as T1days,T1.start as T1start,T1.close as T1close,T1.subj as T1subj, T1.crse as T1crse,T2.term,T2.cid,T2.days,T2.start,T2.close,T2.subj,T2.crse,T2.building,T2.room from (select * from summer)T1,(select * from summer)T2  where T1.building = T2.building and T1.room = T2.room and T1.term = T2.term and T1.cid != T2.cid and (T1.subj != T2.subj or T1.crse != T2.crse) and (Cast(T1.start as float) >= Cast(T2.close as float) or Cast(T2.start as float) >= Cast(T1.close as float)) order by T1.term,T1.cid,T2.cid,T1.days,T2.days,T1.start,T2.start)) T, take where T.cid = take.cid and T.term = take.term order by T1term,T1cid,T1days,T1start,T1close,T1subj,T1crse,T.cid,days,start,close,subj,crse;")
    temp = c.fetchall()
    temp1 = []
    for row in range(len(temp)):
        t1 = temp[row][2]
        t2 = temp[row][8]
        for i in range(len(t1)):
            if t1[i] in t2:
                temp1.append(temp[row])
                break
    unique = []
    temp2 = []
    for row in range(len(temp1)):
        pos = temp1[row][1] + temp1[row][7]
        neg = temp1[row][7] + temp1[row][1]
        if (pos in unique) or (neg in unique): continue
        else:
            unique.append(pos)
            temp2.append(temp1[row])
    results.write("The conflict courses in summer session: \n")
    results.write("\n")
    results.write("Time conflict pairs: \n")
    results.write("\n")
    for i in range(len(temp2)):
        results.write("  " + str(temp2[i][0]) + " " + str(temp2[i][1]) + " " + str(temp2[i][2]) + " "  + str(temp2[i][3]) + " "  + str(temp2[i][4]) + " "  + str(temp2[i][5]) +  " "  + str(temp2[i][6]) + " "  + str(temp2[i][13]) + " "  + str(temp2[i][14]) + "\n") 
        results.write("  " + str(temp2[i][0]) + " " + str(temp2[i][7]) + " " + str(temp2[i][8]) + " "  + str(temp2[i][9]) + " "  + str(temp2[i][10]) + " "  + str(temp2[i][11]) +  " "  + str(temp2[i][12]) + " "  + str(temp2[i][13]) + " "  + str(temp2[i][14]) + "\n")
        results.write("\n")
    c.execute("drop table summer;")
    c.execute("select * from status order by term;")
    temp = c.fetchall()
    c.execute("create table if not exists stat1(sid text, term text, major text, class text, level text);")
    arrange = []
    for i in range(len(temp)):
        if '06-' in temp[i][1][4:]:
            arrange.append(temp[i])
    for i in range(len(arrange)):
        term = arrange[i][1][0:6]
        c.execute("insert into stat1(sid,term,major,class,level) values('" + str(arrange[i][0]) +  "','" + str(term) + "','" + str(arrange[i][2]) + "','" + str(arrange[i][3]) + "','" + str(arrange[i][4]) + "');")
    c.execute("select sid,F.term,F.cid,subj,crse,sec from (select T.sid,T.term,major,class,level,cid,status from (select distinct * from stat1)T, take where T.sid = take.sid and T.term = take.term order by T.term,cid,T.sid)F,courses where F.cid = courses.cid and F.term = courses.term order by F.term,class,sid;")
    temp = c.fetchall()
    arrange1 = []
    for i in range(len(temp)-1):
        if temp[i][0] == temp[i+1][0] and temp[i][2] != temp[i+1][2]:
            arrange1.append([temp[i],temp[i+1]])
    unique = []
    temp2 = []
    for j in range(len(arrange1)):
        pos = arrange1[j]
        if (pos in unique) : continue
        else:
            unique.append(pos)
            temp2.append(arrange1[j])
    results.write("---------------------------------------------------------------------------------\n")
    results.write("Students conflict pairs: \n")
    results.write("\n")
    for j in range(len(temp2)):
        results.write(str(temp2[j][0][0]) + " " + str(temp2[j][0][1]) + " " + str(temp2[j][0][2]) + " " + str(temp2[j][0][3]) + " " + str(temp2[j][0][4]) + "\n" + str(temp2[j][1][0]) + " " + str(temp2[j][1][1]) + " " + str(temp2[j][1][2]) + " " + str(temp2[j][1][3]) + " " + str(temp2[j][1][4]) + " " + "\n")
        results.write("\n")
    c.execute("drop table stat1;")


def partF(c):
    results = open("resultsPartF.txt", "w")
    c.execute("select * from status order by term;")
    temp = c.fetchall()
    c.execute("create table if not exists stat1(sid text, term text, major text, class text, level text);")
    for i in range(len(temp)):
        term = temp[i][1][0:6]
        c.execute("insert into stat1(sid,term,major,class,level) values('" + str(temp[i][0]) +  "','" + str(term) + "','" + str(temp[i][2]) + "','" + str(temp[i][3]) + "','" + str(temp[i][4]) + "');")
    c.execute("select T.sid,cid,T.term,studentunits,grade,major from (select sid,take.cid,take.term,sec,crse,studentunits,grade from courses,take where subj = 'ABC' and take.cid = courses.cid and take.term = courses.term) T,stat1 where T.sid = stat1.sid and T.term = stat1.term group by T.sid,cid,T.term,sec,crse,studentunits,grade,major order by major;")
    temp = c.fetchall()
    c.execute("create table if not exists major(sid text,cid text,term text,units text,grade text,major text);")
    for i in range(0,len(temp)):
        calunit = calcUnits(temp[i][4],temp[i][3])
        calgrade = scoreGrade(temp[i][4])
        c.execute("insert into major(sid,cid,term,units,grade,major) values ('" + str(temp[i][0]) + "','" + str(temp[i][1]) + "','" + str(temp[i][2]) + "','" + str(calunit) + "','" + str(calgrade) + "','" + str(temp[i][5]) + "');")
    c.execute(" (select sum(cast(units as float)*cast(grade as float))/sum(cast(units as float)) as A, major from major where cast(grade as float) >= 0 group by major) Except (select T1.A, T1.major from (select sum(cast(units as float)*cast(grade as float))/sum(cast(units as float)) as A, major from major where cast(grade as float) >= 0 group by major) T1, (select sum(cast(units as float)*cast(grade as float))/sum(cast(units as float)) as A, major from major where cast(grade as float) >= 0 group by major) T2 where T1.A < T2.A);")
    temp1 = c.fetchall()
    c.execute(" (select sum(cast(units as float)*cast(grade as float))/sum(cast(units as float)) as A, major from major where cast(grade as float) >= 0 group by major) Except (select T2.A, T2.major from (select sum(cast(units as float)*cast(grade as float))/sum(cast(units as float)) as A, major from major where cast(grade as float) >= 0 group by major) T1, (select sum(cast(units as float)*cast(grade as float))/sum(cast(units as float)) as A, major from major where cast(grade as float) >= 0 group by major) T2 where T1.A < T2.A);")
    temp2 = c.fetchall()
    results.write("The best performed major:\n")
    for i in range(len(temp1)):
        results.write(str(temp1[i][1]) + " : " + str(temp1[i][0]) + "\n")
    results.write("---------------------------------------------------------------------------------\n")
    results.write("The worst performed major:\n")
    for i in range(len(temp2)):
        results.write(str(temp2[i][1]) + " : " + str(temp2[i][0]) + "\n")
    c.execute("drop table stat1;")
    c.execute("drop table major;")


def partG(c):
    results = open("resultsPartG.txt", 'w')
    # get list of all majors
    c.execute("SELECT DISTINCT MAJOR FROM STATUS ORDER BY MAJOR")
    majorsList = c.fetchall()
    counts = []
    abcMAJORS = 0
    #GET EACH STUDENT AND THEIR STATUS FOR EACH TERM, ORDER BY SID, TERM
    c.execute("SELECT SID, TERM, MAJOR FROM STUDENT NATURAL JOIN STATUS ORDER BY SID, TERM ;")
    temp = c.fetchall()
    index = [] 
    pastMajors = []
    #get indexes where the same student changed major
    for i in range(0, len(temp) - 1): 
        if temp[i][0] == temp[i + 1][0] and temp[i][2] != temp[i+1][2]:
            index.append(i)
        #to count number of students in ABC MAJOR
        if temp[i][0] != temp[i + 1][0] and "ABC" in temp[i][2]:
            abcMAJORS += 1
    #for each index we just retrieved, check if they switched from a non ABC MAJOR to an ABC major
    for i in range(0, len(index)):
        if "ABC" not in temp[int(index[i])][2] and "ABC" in temp[int(index[i]) + 1][2] : #if ABC not in index, but in index + 1, add index to list
            pastMajors.append(temp[index[i]][2])
    #count the number of occurances of each major in the list        
    for i in range(0, len(majorsList)):                         
        x = pastMajors.count(majorsList[i][0])
        counts.append(x)
    x = sum(counts)
    results.write("% OF ABC MAJORS THAT ARE TRANSFERS\n")
    results.write(str((float(x) / float(abcMAJORS)) * 100) + "\n")
    c.execute("CREATE TABLE IF NOT EXISTS TEMPTABLE(MAJOR text,PERCENT float)")
    #order majors by percent
    for i in range(3, len(counts)):
        c.execute("INSERT INTO TEMPTABLE(MAJOR, PERCENT) VALUES('" + str(majorsList[i][0]) + "'," + str(float(counts[i]) / float(x)) + ");")
    c.execute("SELECT MAJOR, PERCENT FROM TEMPTABLE ORDER BY PERCENT DESC ;") 
    temp = c.fetchall()
    c.execute("DROP TABLE TEMPTABLE;")
    results.write("TOP 5 MAJORS THAT TRANSFERRED INTO ABC: \n")
    results.write("MAJOR                         PERCENT \n")
    results.write("-------------------------------------------------------------\n")
    for i in range(0, 5):
        spacer = ""
        spacerLen = 30 - len(temp[i][0])
        for j in range(0, spacerLen):
            spacer = spacer + " "
        results.write(str(temp[i][0]) + spacer + str(temp[i][1] * 100) + "\n")

        
getNames()
with connect('dbname= testdb user=testuser password = pwd') as Con:
    with Con.cursor() as Cursor:
        open('resultsPartA.txt', 'w').close() #clear old data from text file
        open('resultsPartB.txt', 'w').close() #^
        partA(Cursor)
        partB(Cursor)
        partC(Cursor)
        partD(Cursor)
        partE(Cursor)
        partF(Cursor)
        partG(Cursor)



 
