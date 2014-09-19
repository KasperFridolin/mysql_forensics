import argparse
import time
import os

dbType = {"00": "Unknown",
          "01": "DIAB_ISAM",
          "02": "Hash",
          "03": "MISAM",
          "04": "PISAM",
          "05": "RMS_ISAM",
          "06": "Heap",
          "07": "ISAM",
          "08": "MRG_ISAM",
          "09": "MyISAM",
          "0a": "MRG_MyISAM",
          "0b": "Berkeley",
          "0c": "InnoDB",
          "0d": "Gemini",
          "0e": "NDBCluster",
          "0f": "Example_DB",
          "10": "Archive_DB",
          "11": "CSV_DB",
          "12": "Federated_DB",
          "13": "Blackhole_DB",
          "14": "Partition_DB",
          "15": "Binlog",
          "16": "Solid",
          "17": "PBXT",
          "18": "Table_Function",
          "19": "MemCache",
          "1a": "Falcon",
          "1b": "Maria",
          "1c": "Performance_Schema",
          "2a": "First_Dynamic",
          "7f": "Default"}

dataType = {"00": "Decimal",
            "01": "Tiny",
            "02": "Short",
            "03": "Int",
            "04": "Float",
            "05": "Double",
            "06": "Null",
            "07": "Timestamp",
            "08": "Longlong",
            "09": "Int24",
            "0a": "Date",
            "0b": "Time",
            "0c": "Datetime",
            "0d": "Year",
            "0e": "Newdate",
            "0f": "Varchar",
            "10": "Bit",
            "11": "Timestamp2",
            "12": "Datetime2",
            "13": "Time2",
            "f6": "Newdecimal",
            "f7": "Enum",
            "f8": "Set",
            "f9": "Tiny_Blob",
            "fa": "Medium_Blob",
            "fb": "Long_Block",
            "fc": "Blob",
            "fd": "Var_String",
            "fe": "String",
            "ff": "Geometry"}

keyType = {"1b00": "Primary Key",
           "1b40": "Primary Key Auto_Increment",
           "4b00": "Prmary Key Auto_Increment Not NULL",
           "1b80": "Foreign Key"}
          

def parse_tableInformation(myFile):
    myFile.seek(int("03", 16),0) #jump to offset
    return myFile.read(1).encode("hex") #Storage Engine of table


def parse_keyInformation(myFile, numberOfCols):
    myFile.seek(int("1000", 16),0) #jump to the beginning of this block
    info = myFile.read(int("200",16)).encode("hex") #read the complete block as string
    keys = int(info[:2],16) #number of keys
    keyFields = int(info[2:4],16) #number of fields as keys (incl. fk)
    startTitles = info.find("ff")+2 #jump to the start of col descriptions
    endTitles = info.rfind("ff") #jump to the end of col descriptions
    titles = info[startTitles:endTitles].split("ff")
    fields = info[28:startTitles-2]
    splitfields = []
    for i, c in enumerate(fields):#parses the key information
        if int(c,16) <= numberOfCols and fields[i+1:i+3]=="80":
            splitfields.append(fields[i:i+17])
    details = []
    for i, element in enumerate(splitfields):
        tmp = []
        tmp.append(element[:1]) #column
        tmp.append(element[9:13]) #type of key (pk or fk)
        tmp.append(titles[i])
        details.append(tmp)
    return details

def parse_fields(myFile):
    completeFields = []
    myFile.seek(int("2101", 16),0) 
    cols = myFile.read(2).encode("hex") # number of columns in the table
    s = myFile.read().encode("hex")#reading the rest of the file
    i = s.find("00ff")+2 #last col value byte plus one
    y = 34*int(cols,16) #17 Byte per col multiplite by number of cols
    startValues = s[(i-y):] #from first col entry till end
    startTitles = s[i+2:]
    colValues = [] 
    x = 0
    for element in range(int(cols,16)): #to get the 17 Bytes of each column
        colValues.append(startValues[x:(x+34)])
        x += 34
    titles = startTitles.split("ff")
    titleList = [] #titles of each column
    for element in titles: #to get the column titles as string
        if element == "00": continue
        c = ""
        for stri in range(0, len(element), 2):
            c += chr(int(element[stri:(stri+2)], 16))
        titleList.append(c)
    detColVal = []    #detailled information about each column
    for element in colValues: #to parse the detail information about eacht column, like length and datatype
        colValueTuple = []
        colValueTuple.append(element[6:8]) #length of the column
        colValueTuple.append(element[26:28]) #datatype
        detColVal.append(colValueTuple)
    keys = parse_keyInformation(myFile, int(cols,16))
    for i, title in enumerate(titleList):
        completeFields.append([title, detColVal[i]])
    return completeFields, keys

def print_table(fields, keys, fileName, se):
    print "Reconstruction of table:",fileName[fileName.rfind("/")+1:fileName.find(".")]
    for i, field in enumerate(fields): 
        print "Column ",i+1,":",field[0]," ",dataType[field[1][1]], "(", int(field[1][0],16),")" 
    for key in keys:
        if key[1] in keyType: print "Column ",key[0], " is ", keyType[key[1]]
        else: print "Column ",key[0], " is a Key Column, but type is unknown"
    print "Storage Engine: ",dbType[se]    
    print ""

def read_frmfile(path):
    try:
        for frm in os.listdir(path):
            if frm.endswith(".frm"):  
                with open(path+"/"+frm, "rb") as f:
                    fields = parse_fields(f)
                    fi = fields[0]
                    ke = fields[1]
                    print_table(fi, ke, f.name, parse_tableInformation(f))
    except OSError:
        print "----- ERROR -----"
        print "Path not found!" 
        print "----- ERROR -----"

def main():
        parser = argparse.ArgumentParser(description="This script reconstruct the structe of the database tables from .frm files",
                                         epilog="And that's how you should do it ;)")
        parser.add_argument("PATH", help="The path to the .frm files. I.e. /var/lib/mysql/<database_name>")
        args = parser.parse_args()
        read_frmfile(args.PATH)

if __name__ == "__main__":
   main()

                
