#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "HeapFile.h"
#include "Defs.h"
#include <iostream>

DBFile::DBFile() {
    dbInstance = NULL;
}

DBFile ::~DBFile() {
    cout << "DBFile being destroyed\n";
    Close();
}


int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
    //If the file type is heap
    dbInstance = new HeapFile();
    dbInstance->Create(f_path, f_type, startup);
    return 1;
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    dbInstance->Load(f_schema, loadpath);
}

int DBFile::Open(const char *f_path) {
    dbInstance = new HeapFile();
    dbInstance->Open(f_path);
}

void DBFile::MoveFirst() {
    dbInstance->MoveFirst();
}

int DBFile::Close() {
    dbInstance->Close();

    delete dbInstance;
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
