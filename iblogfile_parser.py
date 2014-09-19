import argparse
import time

dataManipulationType = {"0b": "Insert",
                        "1c": "Update",
                        "0e": "Delete"}


#Since the file header, Checkpoints etc are 2048 Bytes in total the first
#block starts at 0x800.
def jump_firstBlock(myFile):
        return myFile.seek(2048,0)

#Jump back to 0x0 and read the complete 2048 Bytes of the header.
def parse_fileHeader(myFile):
        myFile.seek(0,0)
        return myFile.read(2048).encode("hex")
        
#Reads the 512 Byte of each block in the given file and parsed the block header incl. detailled information like the block no, the block entry
#as well as the block trailer. It returns a list with all block elements (no empty blocks). One block element is a single list
#with 3 elements (header, entry, trailer)
def parse_blocks(myFile):
        jump_firstBlock(myFile)
        blocks = []
        i = 0
        while myFile:
                #Block_Header details
                tmpBlockHeaderNo = myFile.read(4).encode("hex")
                tmpBlockNrWrittenBytes = myFile.read(2).encode("hex")
                tmpBlockOffsetOfLRGStart = myFile.read(2).encode("hex")
                tmpBlockNrActiveCheckpoints = myFile.read(4).encode("hex")
                tmpBlockHdrSize = myFile.read(2).encode("hex")
                #Block_Entry details
                tmpBlockEntry = myFile.read(494).encode("hex")
                #Block_Trailer details
                tmpBlockTrailer = myFile.read(4).encode("hex")
                if tmpBlockOffsetOfLRGStart == "0000" and tmpBlockHeaderNo != "00000000" and tmpBlockHeaderNo != "0000": continue
                blockHeader = [tmpBlockHeaderNo, tmpBlockNrWrittenBytes, tmpBlockOffsetOfLRGStart, tmpBlockNrActiveCheckpoints, tmpBlockHdrSize]
                blocks.insert(i, [blockHeader, tmpBlockEntry, tmpBlockTrailer])
                i += 1
                if tmpBlockHeaderNo == "00000000": break
        return blocks        

#returns the block header number of a given block.
def get_blockHeaderNo(block):
        return block[0][0]

#returns the number of written bytes of this block.
def get_blockNrWrittenBytes(block):
        return block[0][1]

#returns the offset of the entry block start.
def get_blockOffsetOfLRGStart(block):
        return block[0][2]

#returns the active checkpoint of this block.
def get_blockActiveCheckpoints(block):
        return block[0][3]

#returns the hdr size of this block
def get_blockHdrSize(block):
        return block[0][4]

#returns the complete block header as a string. MAYBE USELESS
def get_BlockHeaderAsString(block):
        return get_blockHeaderNo(block)+get_blockNrWrittenBytes(block)+get_blockOffsetOfLRGStart(block)+get_blockActiveCheckpoints(block)+get_blockHdrSize(block)

#returns the block entry type from the entry start (log entry type) to the end. Needs the offset of the log entry type and a single block.
def get_logEntryReconstruction(offset, blockEntry):
        tmpBlock = get_BlockHeaderAsString(blockEntry)+blockEntry[1]
        return tmpBlock[int(offset, 16)*2:]

#references a given block entry (start from log entry type) with the header number
def get_mlog_undo_insert_Entry(block):
        reference = get_logEntryReconstruction(get_blockOffsetOfLRGStart(block), block)
        if reference.startswith("94") or reference.startswith("14"): 
                return [get_blockHeaderNo(block), reference]

#
def set_mlog_undo_insert_list(blocks):
        mlog_undo_list = []
        i = 0
        for block in blocks:
                if get_mlog_undo_insert_Entry(block) is not None:
                        mlog_undo_list.insert(i, get_mlog_undo_insert_Entry(block))
                        i += 1
        return mlog_undo_list

#parses the block entry from the first entry to the end i.e. from 0x94
def parse_mlog_undo_insert_entry(mlog_undo_list):
        detailled_mlog_undo_insert_list = []
        for element in mlog_undo_list:
                tmpString = element[1]
                #                                       BlockNo,    Log Entry Type,TableSpace ID,  Page ID      ,  Length of Entry, Data Manip Type,  Table ID        , Rest               
                detailled_mlog_undo_insert_list.append([element[0], tmpString[:2], tmpString[2:6], tmpString[6:8], tmpString[8:12],
                                                        tmpString[12:14], tmpString[14:18], tmpString[18:]])
        return detailled_mlog_undo_insert_list


def get_logEntryType(detailled_mlog_undo_insert_list_element):
        return detailled_mlog_undo_insert_list_element[1]

def get_tableSpaceID(detailled_mlog_undo_insert_list_element):
        return detailled_mlog_undo_insert_list_element[2]

def get_pageID(detailled_mlog_undo_insert_list_element):
        return detailled_mlog_undo_insert_list_element[3]

def get_lengthOfLogEntry(detailled_mlog_undo_insert_list_element):
        return detailled_mlog_undo_insert_list_element[4]

def get_dataManipulationType(detailled_mlog_undo_insert_list_element):
        return detailled_mlog_undo_insert_list_element[5]

def get_tableID(detailled_mlog_undo_insert_list_element):
        return detailled_mlog_undo_insert_list_element[6]

def get_rest(detailled_mlog_undo_insert_list_element):
        return detailled_mlog_undo_insert_list_element[7]

def set_insertStatementList(detailled_mlog_undo_insert_list):
        insertList = []
        for element in detailled_mlog_undo_insert_list:
                if get_dataManipulationType(element) == "0b": insertList.append(element)
        return insertList

def set_updateStatementList(detailled_mlog_undo_insert_list):
        updateList = []
        for element in detailled_mlog_undo_insert_list:
                if get_dataManipulationType(element) == "1c": updateList.append(element)
        return updateList

def set_deleteStatementList(detailled_mlog_undo_insert_list):
        deleteList = []
        for element in detailled_mlog_undo_insert_list:
                if get_dataManipulationType(element) == "0e": deleteList.append(element)
        return deleteList

def parse_detailled_update_information(updateList):
        splitList = []
        printList = []
        tableIDs = []
        for element in updateList:
                tmpList = []
                hl = []
                tmp = []
                tmpList.append(element[0])                                                              #BlockNo
                tmpList.append(element[1])                                                              #Log Entry Type
                tmpList.append(element[2])                                                              #Tablespace ID
                tmpList.append(element[3])                                                              #Page ID
                tmpList.append(element[4])                                                              #Length of the Log Entry
                tmpList.append(element[5])                                                              #Data Manipulation Type
                tmpList.append(element[6])                                                              #Table ID
                tableIDs.append(element[6])
                hl = parse_transIDAndRBPFields(element[7],0)                                            #last transaction id and rollback pointer
                tmpList += hl[0]
                pointer = hl[1] 
                pointer += 2                                                                            #2 Byte unbekannt
                hl = parse_pkInformation(element[7], pointer)
                tmpList += hl[0]
                pkLength = int(hl[0][0],16)*2
                pointer = hl[1]
                hl = parse_numberOfUpdatesFields(element[7], pointer)
                if hl is None: continue
                tmpList += hl[0]
                pointer = hl[1]
                tmp += hl[0]
                hl = parse_newUpdateValue(element[7], element[2], pkLength)
                tmpList += hl[0]
                pointer = hl[1]
                tmp += hl[2]
                printList.append(tmp)
                splitList.append(tmpList)
        return splitList, printList, tableIDs
                

def parse_newUpdateValue(element, tablespaceID, pkLength):
        tmp = []
        hl = []
        printList = []
        pointer = 0
        newStmt = element[element.find("26"+tablespaceID):]
        tmp.append(newStmt[:pointer+2])                  # Log Entry Type
        pointer += 2
        tmp.append(newStmt[pointer:pointer+4])           # Tablespace ID
        pointer += 4
        tmp.append(newStmt[pointer:pointer+2])           # Page ID
        pageID = newStmt[pointer:pointer+2]
        pointer += 2
        hl = parse_mlog_comp_rec_insert(newStmt, pointer, tablespaceID, pageID, pkLength)
        tmp += hl[0]
        pointer = hl[1]
        printList = hl[3]
        return tmp, pointer, printList


def print_updates(printList, tableIDs, tableRef):
        updatedFields = 0
        colsOfStmt = 0
        start = 4
        for i, update in enumerate(printList):
                updatedFields = int(update[0],16)
                colsOfStmt = update[4]*updatedFields
                start *= updatedFields
                if tableIDs[i] in tableRef: print "UPDATE ", tableRef[tableIDs[i]]
                else: print "UPDATE <unknown>"
                print "SET column",int(update[1],16)-1,"=",update[(start+int(update[1],16)-1)]
                print "WHERE column",int(update[1],16)-1,"=",update[3],");"
                print ""
           
def parse_numberOfUpdatesFields(element, pointer):
        tmp = []
        tmp.append(element[pointer:pointer+2])                                  #Number of updated fields
        pointer += 2
        if element[pointer-2:pointer] == "": return
        for field in range(int(element[pointer-2:pointer], 16)):
                tmp.append(element[pointer:pointer+2])                          #ID of the updated field
                pointer += 2
                length = int(element[pointer:pointer+2], 16)
                tmp.append(length)                                              #Length of updated field
                pointer += 2
                tmp.append(read_hexdump(element[pointer:pointer+length*2]))     #Value of the updated field
                pointer += length*2
        return tmp, pointer
        


#returns a list with the all insert statement splitted into the detailled information.
#hier muss ich mir noch was fuer die false positives ueberlegen. Eventuell einfach extra wo hinspeichern
def parse_detailled_insert_information(insertList):
        splitList = []
        printList = []
        tableIDs = []
        for element in insertList:
                tmpList = []
                metaList = []
                hl = []
                #Metadata
                if (element[0])[:2] != "80": continue
                metaList.append(element[0])                                             #BlockNo
                metaList.append(element[1])                                             #Log Entry Type
                metaList.append(element[2])                                             #Tablespace ID
                metaList.append(element[3])                                             #Page ID
                metaList.append(element[4])                                             #Length of the Log Entry
                metaList.append(element[5])                                             #Data Manipulation Type
                metaList.append(element[6])                                             #Table ID
                tableIDs.append(element[6])
                #Specific data
                hl = parse_insert_mlog_undo_insert(element[7], 0, element[2], element[3])
                if hl is not None:
                        splitList.append([metaList,hl[0]])
                        printList.append(hl[3])
        return splitList, printList, tableIDs

#Pareses mlog_undo_insert Statements
def parse_insert_mlog_undo_insert(element, pointer, tablespaceID, pageID):
                metaList = []
                printList = []
                hl = parse_pkInformation(element, pointer)
                tmpList = hl[0]
                pkLength = int(hl[0][0],16)*2
                pointer = hl[1]
                pointer += 2                                    #Unbekannt 1Byte
                hl = parse_varTablespaceID(element, tablespaceID, pointer)
                tmpList += hl[0]
                pointer = hl[1]
                pointer += 2                                    #Unbekannt 1Byte
                hl = parse_mlog_comp_rec_insert(element, pointer, tablespaceID, pageID, pkLength)
                if hl is None: return
                tmpList += hl[0]
                pointer = hl[1]
                metaList += hl[2]
                printList += hl[3]
                tl = split_multipleInserts(element[pointer:], tablespaceID, pageID)
                if tl is not None:
                        for sub in tl:
                                if sub != "":
                                        hl = parse_insert_mlog_undo_insert(sub, 10, tablespaceID, pageID)
                                        tmpList += hl[0]
                                        pointer = hl[1]
                                        metaList += hl[2]
                                        printList += hl[3]
                return tmpList, pointer, metaList, printList
                
#Parses mlog_comp_rec_insert Statements                
def parse_mlog_comp_rec_insert(element, pointer, tablespaceID, pageID, pkLength):
                hl = []
                tmpList = []
                parseList = []
                metaList = []
                printList = []
                parseList.append(pkLength)
                hl = parse_fieldCount(element, pointer)
                tmpList += hl[0]
                pointer = hl[1]
                fieldsInEntry = hl[0][0]                      #2Byte
                parseList.append(fieldsInEntry)
                uniqueFields = hl[0][1]                       #Number of unique fields 2Byte
                parseList.append(uniqueFields)
                printList.append(fieldsInEntry)
                hl = parse_uniqueFieldLength(element, pointer, uniqueFields)
                tmpList += hl[0]
                parseList += hl[0]
                pointer = hl[1]
                hl = parse_transIDAndRBPLength(element, pointer)
                tmpList += hl[0]
                pointer = hl[1]
                metaList.append(hl[0][0])                        #length of the transaction id as metadata to use in delete recovery
                metaList.append(hl[0][1])                        #length of the data rollback pointer as metadata to use in delete recovery
                hl = parse_nonUniqueFieldLength(element, pointer, fieldsInEntry-uniqueFields)
                tmpList += hl[0]
                pointer = hl[1]
                parseList += hl[0]
                hl = parse_offset(element, pointer)
                tmpList += hl[0]
                pointer = hl[1]
                hl = parse_realLengthofData(element, pointer, fieldsInEntry-uniqueFields)
                tmpList += hl[0]
                pointer = hl[1]
                parseList += hl[0]
                pointer +=10                                    #Unbekannt 5Byte
                hl = parse_uniqueFields(element, pointer, uniqueFields, pkLength)
                tmpList += hl[0]
                pointer = hl[1]
                printList += hl[0]
                hl = parse_transIDAndRBPFields(element, pointer)
                tmpList += hl[0]
                pointer = hl[1]
                totalLength = sort_structure(parseList)
                hl = parse_FieldHexdump(element, totalLength, pointer)
                if hl is None: return
                tmpList += hl[0]
                pointer = hl[1]
                printList += hl[0]
                return tmpList, pointer, metaList, printList
        
#Prints all insert statements as SQL-Statement. 
def print_inserts(printList, tableIDs, tables):
        for i, y in enumerate(printList):
                cols = y[0]
                start = 1
                end = cols+1
                if tableIDs[i] in tables: print "INSERT INTO", tables[tableIDs[i]] ,"VALUES ("
                else: print "INSERT INTO <unknown> VALUES ("
                for x in range(len(printList[i])):
                        if len(printList[i][start:end]) > 0: print ", ".join("%s" %s for s in printList[i][start:end]), ","
                        start = end+1
                        end += cols+1
                print ");"
                print ""

#Parses the hexdump
def parse_FieldHexdump(element, totalLength, pointer):
        if totalLength is None: return
        tmp = []
        for l in totalLength:
                hexdump = element[pointer:pointer+int(l[1],16)*2]
                pointer += int(l[1],16)*2
                if l[0] == "var":
                        v = read_hexdump(hexdump)
                        tmp.append(v)
                if l[0] == "fix":
                        f = read_int(hexdump)
                        tmp.append(f)
        return tmp, pointer

#Parses the unique fields within the entry
def parse_uniqueFields(element, pointer, numberOfUniFields, pkLength):
        tmp = []
        if numberOfUniFields <10:
                for i in range(numberOfUniFields):
                        value = element[pointer:pointer+pkLength]
                        if not value.startswith("8") and not value.startswith("00"):
                                pointer += 2
                        tmp.append(read_int(element[pointer:pointer+pkLength]))
                        pointer += pkLength
        return tmp, pointer

#Parses the real length of the data of the related fields within the entry
def parse_realLengthofData(element, pointer, fieldsInEntry):
        tmp = []
        if fieldsInEntry < 20: #UGLY AYM!!!!
                for i in range(fieldsInEntry):
                        tmp.append(element[pointer:pointer+2])
                        pointer += 2
        return tmp, pointer

#Parses the offset
def parse_offset(element, pointer):
        tmp = []
        tmp.append(element[pointer:pointer+4])
        pointer += 4
        for field in range(4):
                tmp.append(element[pointer:pointer+2])
                pointer += 2
        return tmp, pointer

#Parses the transaction ID and the Data Rollback Pointer
def parse_transIDAndRBPFields(element, pointer):
        tmp = []
        tmp.append(element[pointer:pointer+12])
        pointer += 12
        tmp.append(element[pointer:pointer+14])
        pointer += 14
        return tmp, pointer

#Parses the length of the transaction ID and the Data Rollback Pointer                
def parse_transIDAndRBPLength(element, pointer):
        tmp = []
        tmp.append(element[pointer:pointer+4])
        pointer += 4
        tmp.append(element[pointer:pointer+4])
        pointer += 4
        return tmp, pointer

#Parses the length of the non unique fields
def parse_nonUniqueFieldLength(element, pointer, numberOfFields):
        tmp = []
        if numberOfFields < 10: #UGLY AYM!!!!
                for i in range(numberOfFields):
                        tmp.append(element[pointer:pointer+4])
                        pointer += 4
        return tmp, pointer

#Parses the length of the unique fields
def parse_uniqueFieldLength(element, pointer, numberOfUniqueFields):
        tmp = []
        if numberOfUniqueFields < 10: #UGLY AYM!!!!
                for i in range(numberOfUniqueFields):
                        tmp.append(element[pointer:pointer+4])
                        pointer += 4
        return tmp, pointer
                        
#Parses the number of all data fields in this entry
def parse_fieldCount(element, pointer):
        tmp = []
        tmp.append(int(element[pointer:pointer+4],16)-2)
        pointer += 4
        tmp.append(int(element[pointer:pointer+4],16))
        pointer += 4
        return tmp, pointer

#Parses the information of the primary key        
def parse_pkInformation(element, pointer):
        tmp = []
        tmp.append(element[pointer:pointer+2]) # Length of the primary key field
        pkLength = int(element[pointer:pointer+2], 16)*2
        pointer += 2
        tmp.append(element[pointer:pointer+pkLength]) #primary key of the inserted field
        pointer += pkLength
        return tmp, pointer

#Parses the variable tablespace ID
def parse_varTablespaceID(element, tablespaceID, pointer):
        tmp = []
        if tablespaceID == element[pointer:pointer+4]:
                tmp.append(element[pointer:pointer+4])
                pointer +=4
        else:
                tmp.append(element[pointer:pointer+2])
                pointer +=2
        return tmp, pointer

#Splits the block into each insert entry if more than one entry exisits        
def split_multipleInserts(values, tableID, pageID):
        if values.startswith("94"+tableID+pageID):
                return values.split("94"+tableID+pageID)
        if values.startswith("14"+tableID+pageID):
                return values.split("14"+tableID+pageID)

        
#Reads the int value of the given hexdump
def read_int(hexdump):
        return int(hexdump[2:], 16)

#Reads the char of the fiven hexdump
def read_hexdump(hexdump):
        c = ""
        for i in range(0, len(hexdump), 2):
                c += chr(int(hexdump[i:i+2],16))
        return c

#Sorts the structure of a fiven list to get a better output
def sort_structure(parseList):
        pks = []
        dfs = []
        rls = []
        sorts = []
        pkLength = parseList[0]
        nonUnique = parseList[1]-parseList[2]
        pkCount = parseList[2]
        if len(parseList) <= 3: return
        for i in range(pkCount):
                pks.append(parseList[i+3])
        for i in range(nonUnique):
                dfs.append(parseList[i+3+pkCount])
                if not parseList[i+3+pkCount].endswith("0"): nonUnique -= 1
        for i in range(nonUnique):
                rls.append(parseList[i+3+pkCount+len(dfs)])
        i = 0
        for element in dfs:
                tl = []
                if element.endswith("0"):
                        tl.append("var")
                        tl.append(rls[len(rls)-i-1])
                        sorts.append(tl)
                        i += 1
                else:
                        tl.append("fix")
                        tl.append(element[len(element)-2:])
                        sorts.append(tl)
        return sorts

#returns a list with the all insert statement splitted into the detailled information. 
def parse_detailled_delete_information(deleteList, lengthOfTransactionID, lengthOfRollbackPointer):
        splitList = []
        lengthOfTransactionID *= 2
        lengthOfRollbackPointer *= 2
        rbpEnd = lengthOfTransactionID+lengthOfRollbackPointer+2
        for element in deleteList:
                tmpList = []
                tmpList.append(element[0])                                                              #BlockNo
                tmpList.append(element[1])                                                              #Log Entry Type
                tmpList.append(element[2])                                                              #Tablespace ID
                tmpList.append(element[3])                                                              #Page ID
                tmpList.append(element[4])                                                              #Length of the Log Entry
                tmpList.append(element[5])                                                              #Data Manipulation Type
                tmpList.append(element[6])                                                              #Table ID
                tmpList.append((element[7])[:lengthOfTransactionID])                                    #Last Transaction ID
                tmpList.append((element[7])[lengthOfTransactionID:lengthOfTransactionID+2])             #Unbekannt
                tmpList.append((element[7])[lengthOfTransactionID+2:rbpEnd])                            #Last data rollback pointer
                tmpList.append((element[7])[rbpEnd:rbpEnd+2])                                           #Length of the primary key
                pkLength = int(((element[7])[rbpEnd:rbpEnd+2]),16)*2
                tmpList.append((element[7])[rbpEnd+2:(rbpEnd+2+pkLength)])                              #Affected primary key
                tmpList.append((element[7])[(rbpEnd+8+pkLength):(rbpEnd+10+pkLength)])                  #Length of the primary key field
                pkFieldLength = int(((element[7])[(rbpEnd+8+pkLength):(rbpEnd+10+pkLength)]),16)*2
                tmpList.append((element[7])[(rbpEnd+10+pkLength):((rbpEnd+10+pkLength)+pkFieldLength)]) #primary key of deleted field
                splitList.append(tmpList)
        return splitList

#Prints the delete list
def print_deletes(deleteList, tableRef):
        for delete in deleteList:
                if not delete[0].startswith("80"): continue
                if delete[6] in tableRef: print "DELETE FROM", tableRef[delete[6]]
                else: print "DELETE FROM <unknown>"
                print "WHERE primaryKey =",read_int(delete[len(delete)-1]),";"
                print ""

#Gets the length of the Transaction ID
def get_LengthOfTransactionID(insertDataList):
        return (insertDataList[0][7])[3:]

#Get the length of the Data Rollback Pointer
def get_LengthOfRollbackPointer(insertDataList):
        return (insertDataList[0][8])[3:]

def print_statistics(inserts, updates, deletes):
        print "---- Overview ----"
        print ""
        print "Type\t| Block No"
        print "----\t| --------"
        print "Insert\t|"
        x = 0
        y = 0
        z = 0
        for insert in inserts:
                if not insert[0][0].startswith("80"): continue
                x += 1
                print "\t| ", insert[0][0]
        print "----\t| --------"
        print "Update\t|" 
        #for update in updates:
        #        if not update[0].startswith("80"): continue
        #        y += 1
        #        print "\t| ", update[0]
        print "OUT OF ORDER :)"
        print ""
        print "----\t| --------"
        print "Delete\t|"
        for delete in deletes: 
                if not delete[0].startswith("80"): continue
                z += 1
                print "\t| ", delete[0]
        print "----\t| --------"
        print ""
        print "-- Total number of Insert-Statements found: ", x
        print "-- Total number of Update-Statements found: ", y
        print "-- Total number of Delete-Statements found: ", z
        print ""
                
#opens the ib_logfile                
def read_ib_logfile(ib_logfile, dbName, ibdata, numberOfColumns):
        inserts = []
        updates = []
        deletes = []
        try:
                with open(ib_logfile, "rb") as f:
                        print "Start of analyses:", time.strftime("%d.%m.%Y - %H:%M:%S")
                        print "Results of analysing the", dbName, "database: "
                        print ""
                        detailledList = parse_mlog_undo_insert_entry(set_mlog_undo_insert_list(parse_blocks(f)))
                        print ""
                        print "---- INSERTS ----"
                        hl = parse_detailled_insert_information(set_insertStatementList(detailledList))
                        inserts = hl[0]
                        tableRef = get_tableName(read_ibdata(ibdata, inserts, dbName))
                        print_inserts(hl[1], hl[2], tableRef) 
                        print ""
                        print "---- UPDATES ----"
                        #hl = parse_detailled_update_information(set_updateStatementList(detailledList))
                        #if hl is not None: print_updates(hl[1], hl[2], tableRef)
                        print "OUT OF ORDER :)"
                        print ""
                        print "---- DELETES ----"
                        deletes = hl[0]
                        hl = parse_detailled_delete_information(set_deleteStatementList(detailledList), 6,7) #No specific "printList" like in insert or update parser
                        if hl is not None: print_deletes(hl, tableRef)
                        print_statistics(inserts, deletes, hl)
                        print "End of analyses: ", time.strftime("%d.%m.%Y - %H:%M:%S")
                        f.close()
        except IOError:
                print "----- ERROR -----"
                print "ib_logfile not found!"
                print "----- ERROR -----"

#Reads the given ibdata
def read_ibdata(myFile, splitList, dbName):
        tableIDs = []
        tmp = []
        dbName = dbName.encode("hex") 
        for element in splitList:
                tableIDs.append(get_tableID(element[0]))
        try:
                with open(myFile, "rb") as f:
                        tmp = split_ibdata(f, tableIDs, dbName)
                f.close()
        except IOError:
                print "----- ERROR -----"
                print "ibdata not found!"
                print "----- ERROR -----"
        return tmp

#Splits the ibdata file to get the tableID - tablename faster
def split_ibdata(myFile, tableIDs, dbName):
        tmp = []
        pointer = 0
        while myFile:
                length = len(dbName)/2
                offset = myFile.read(2).encode("hex")
                if offset in tableIDs:
                        dbHex = myFile.read(length).encode("hex")
                        if dbName in dbHex:
                                tmpName = myFile.read(50).encode("hex")
                                myFile.seek(-50,1)
                                tmp.append([offset, tmpName])
                        myFile.seek(-length,1)
                if not offset: break
        return tmp

#Gets the name of the table
def get_tableName(ibdataList):
        tableRef = {}
        for element in ibdataList:
                c = ""
                for i in range(0, len(element[1]), 2):
                        if (int("30",16) <= int((element[1])[i:i+2],16) <= int("39",16)) or (int("41",16) <= int((element[1])[i:i+2],16) <= int("5A",16)) or (int("61",16) <= int((element[1])[i:i+2],16) <= int("7A",16)) or (int("2f",16) == int((element[1])[i:i+2],16)):
                                c += chr(int((element[1])[i:i+2],16)) 
                        else:
                                c += "--"
                if c[c.find("/")+1:c.find("--")] != "": tableRef[element[0]] = c[c.find("/")+1:c.find("--")]
        return tableRef

#Parses the arguments from the command line
def main():
        parser = argparse.ArgumentParser(description="This script interprets the given ib_logfile (ib_logfile0 or ob_logfile_1) and the ibdata1 of a database to reconstruct the used insert, update and delete statements.",
                                         epilog="And that's how you should do it ;)")
        parser.add_argument("-l", default="/var/lib/mysql/ib_logfile0", help="The ib_logile0 file, i.e. /var/lib/mysq/ib_logfile0")
        parser.add_argument("DB", help="The name of the database")
        parser.add_argument("-i", default="/var/lib/mysql/ibdata1", help="The ibdata1 file, i.e. /var/lib/mysql/ibdata1")
        parser.add_argument("-f", default=20, help="The number of max. columns within the tables. This is needed for performance. Default value is 20.")
        args = parser.parse_args()
        read_ib_logfile(args.l, args.DB, args.i, args.f)

if __name__ == "__main__":
   main()

