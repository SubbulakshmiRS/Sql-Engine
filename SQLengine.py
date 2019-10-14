import csv
import sys
import sqlparse
import re
import os

metaData = {}
data = {}
fields = []
tables = []
values = []
conditions = []
projections = []

def main():
    processMetaData()
    if len(sys.argv) < 2 :
        print("No query")
        return    
    query = sys.argv[1]
    parsed = sqlparse.parse(query)
    stmt = parsed[0]
    if str(stmt.tokens[0]) == "select" :
        processStatement(stmt.tokens)  

def processMetaData():
    global metaData, data, fields, tables, values
    f=open("metadata.txt", "r")
    contents =f.readlines()

    new = 0
    tableName = ""
    fields = []

    for line in contents:
        if line == "<begin_table>" :
            new = 1      
        elif new == 1:
            tableName = line
            fields = []
            new = 0
        elif line == "<end_table>" :
            metaData[tableName] = fields
        else :
            fields.append(line)

    f.close()

def processStatement(tokens):
    global metaData, data, fields, tables, values
    data = {}
    fields = []
    tables = []
    values = []
    identifierList = []
    l = sqlparse.sql.IdentifierList(tokens).get_identifiers()
    
    for i in l:
        identifierList.append(str(i))

    function = re.sub("[\(\)]",' ',identifierList[1]).split()
    if (function[0] == '*'):
        for tableName, fieldList in data.items():
            tables.append(tableName)
            fields += fieldList
    else :
        fields = re.sub("[\,]",' ',identifierList[1]).split()
        tables = re.sub("[\,]",' ',identifierList[3]).split()

        """if len(identifierList) == 5:
            where = identifierList[4].split()
            for i in range(1,len(where),2):
                attribute = re.sub("[\=\>\<0-9]",' ',where[i]).split()
                feilds += attribute
                if len(attribute) == 1:
                    processCondition(where)
                elif len(attribute) == 2:
                    processProjections(where)"""


    if( check_tables() == 1):
        print("Error: Table not found")
        return
    state = check_fields() 
    if state == 1:
        print("Error: Field not found")
        return 
    elif state == 2:
        print("Error: Field common to multiple tables present")
        return

    for tableName in tables:
        cartesian_prd(tableName)

    #assumption max and min are for only one column
    if (function[0] == '*'):
        pass
    elif(function[0] == 'max'):
        values = [max(i) for i in zip(*values)]
    elif(function[0] == 'min'):
        values = [max(i) for i in zip(*values)]
    elif(function[0] == 'sum'):
        values = [sum(i) for i in zip(*values)]
    elif(function[0] == 'avg'):
        l = len(values)
        values = [sum(i)/l for i in zip(*values)]
    elif(function[0] == 'distinct'):
        newValues = list(set(values))
    else:
        pass
    printData()

def check_tables():
    global metaData, data, fields, tables, values 
    tables = list(set(tables))
    for i in tables:
        if i in metaData.keys():
            pass
        else :
            return 1

    return 0


def check_fields():
    global metaData, data, fields, tables, values
    fields = list(set(fields))
    temp = fields
    found = []
    for i in fields:
        found.append(-1)

    for fieldName in temp:
        index = 0
        for tableName, fieldList in metaData.items():
            if fieldName in fieldList:
                if found[index] == 1:
                    return 2
                found[index] = 1
        index += 1

    for tableName, fieldList in metaData.items():
        index = 0
        for fieldName in fieldList:
            if fieldName in temp:
                temp.remove(fieldName)
                if tableName in data.keys():
                    data[tableName].append(index)
                else :
                    data[tableName] = []
                    data[tableName].append(index)
            index += 1

    if len(temp) != 0:
            return 1

    return 0

def cartesian_prd(tableName):
    global metaData, data, fields, tables, values
    newValues = []
    filepath = tableName+".csv"
    if (os.path.exists(filepath)):
        f = open(filepath,'rb')
        reader = csv.reader(f) 

        for i in values:
            newRow = i
            for row in reader:
                temp = []
                for field in data[tableName]:
                    temp.append(int(row[field]))
                
            newRow += temp
            newValues.append(newRow)

    values = newValues

def printData():
    global metaData, data, fields, tables, values
    for tableName, fieldList in data.items():
        for i in fieldList :
            print(str(metaData[tableName][i]), end = '\t')

    print("\n")

    for row in values:
        for i in row:
            print(str(i), end = '\t')
        print("\n")


