#include "SortedDBFile.h"
#include <iostream>

SortedDBFile ::~SortedDBFile() {
    cout << "SortedDBFile being destroyed\n";
    Close();
}

SortedDBFile::SortedDBFile() {}


int SortedDBFile::Create(const char *f_path, fType f_type, void *startup)
{
    cout << "create called \n";
    // Assumption: if file already exists, it would be over written.
    // initState(true, f_path);
    return 1;
}

void SortedDBFile::Load(Schema &f_schema, const char *loadpath) {

}

int SortedDBFile::Open(const char *f_path) {
    return 1;
}

void SortedDBFile::MoveFirst() {
}

int SortedDBFile::Close() {
    cout << "Close called.\n";
    // If file or page is null, we have already closed this file.
    if (actualFile == NULL) return 0;

    actualFile->Close();
    delete actualFile;
    actualFile = NULL;

    return 1;
}

void SortedDBFile::Add(Record &rec) {
}

int SortedDBFile::GetNext(Record &fetchme) {
    return 1;
}

int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return 0;
}
