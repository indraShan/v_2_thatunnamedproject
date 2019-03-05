#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "SortedDBFile.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>

DBFile::DBFile() {
    dbInstance = NULL;
}

DBFile ::~DBFile() {
    cout << "DBFile being destroyed\n";
    Close();
}

// Returns true if the file exists at given path.
// false otherwise.
bool DBFile::fileExists(const char *f_path) {
    struct stat buf;
    return (stat(f_path, &buf) == 0);
}

int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    if(f_type == heap){
        dbInstance = new HeapFile();
    }
    else if(f_type == sorted){
        dbInstance = new SortedDBFile();
    }
    return dbInstance->Create(f_path, f_type, startup);
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    dbInstance->Load(f_schema, loadpath);
}


int DBFile::Open(const char *f_path) {
    if (!fileExists(f_path)) return 0;
    // Check the meta data
    std::string f_type;
    std::ifstream file(string(f_path)+".meta");
    if (file.is_open()) {
        // Read contents of the metadata to get file properties
        std::getline(file,f_type);
        file.close();
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
        dbInstance = new SortedDBFile();
    }    
    return dbInstance->Open(f_path);
}

void DBFile::MoveFirst() {
    dbInstance->MoveFirst();
}

int DBFile::Close() {
    if(dbInstance == NULL) return 0;
    
    dbInstance->Close();
    dbInstance = NULL;
    return 1;
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
