#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <map>
#include <fstream>
#include <sstream>

DBFile::DBFile() {
    dbInstance = NULL;
}

DBFile ::~DBFile() {
    cout << "DBFile being destroyed\n";
    Close();
}


int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    if(f_type == heap){
        dbInstance = new HeapFile();
    }
    dbInstance->Create(f_path, f_type, startup);
    return 1;
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    dbInstance->Load(f_schema, loadpath);
}


int DBFile::Open(const char *f_path) {
    // Check the meta data
    std::string f_type, f_name, line, numAtts;
    std::ifstream file(string(f_path)+".meta");
    if (file.is_open()) {
        // Read contents of the metadata to get file properties
        std::getline(file,f_type);
        std::getline(file,f_name);
    }
    else{
        cout << "Couldn't find meta file. Please try creating the database file again.";
        return 0;
    }
    // Create dbInstance based on the file type from meta data
    if(f_type == "heap")
        dbInstance = new HeapFile();    
    else if(f_type == "sorted"){
        // Change to sortedfile
        dbInstance = new HeapFile();
        map<string,Type> typeMap;
        typeMap["String"] =  String;
        typeMap["Int"] =  Int;
        typeMap["Double"] =  Double;
        // Get the number of attributes in orderMaker
        std::getline(file,numAtts);
        int nAtts = std::stoi(numAtts);
        OrderMaker o;
        if(nAtts > 0){
            int attributes[nAtts];
            Type types[nAtts];
            int i = 0;
            // Read each attribute and generate arrays of attributes and their types
            while(std::getline(file,line)){
                std::istringstream iss(line);
                int index;
                string dType;
                if (!(iss >> index >> dType)) { break; }
                attributes[i] = index;
                types[i] = typeMap[dType];
                i++;
            }

            // Create an OrderMaker with the attributes read from file
            o.testing_helper_setAttributes(nAtts, attributes, types);
        }
    }    
    file.close();
    return dbInstance->Open(f_path);
}

void DBFile::MoveFirst() {
    dbInstance->MoveFirst();
}

int DBFile::Close() {
    if(dbInstance == NULL) return 0;
    
    dbInstance->Close();
    dbInstance = NULL;
}


void DBFile::Add(Record &rec) {
    dbInstance->Add(rec);
}

int DBFile::GetNext(Record &fetchme) {
    return dbInstance->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return dbInstance->GetNext(fetchme, cnf, literal);
}
