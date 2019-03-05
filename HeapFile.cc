#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "HeapFile.h"
#include "Defs.h"
#include <iostream>
#include <fstream>

HeapFile::HeapFile() {
    actualFile = NULL;
    currentPage = NULL;
}

HeapFile ::~HeapFile() {
    cout << "DBFile (heap) being destroyed\n";
    Close();
}

// Returns true if the file exists at given path.
// false otherwise.
bool HeapFile::fileExists(const char *f_path) {
    struct stat buf;
    return (stat(f_path, &buf) == 0);
}

void HeapFile::initState(bool createFile, const char *f_path) {
    actualFile = new File();
    currentPage = new Page();
    currentReadPageIndex = 0;
    lastReturnedRecordIndex = -1;
    writeInPlace = false;
    // By default we start off in read mode.
    inReadMode = true;
    // TODO: This is wrong, but okay for now.
    fileType = heap;

    // Create or open the file.
    actualFile->Open(createFile == true ? 0 : 1, f_path);
    currentPage->EmptyItOut();
    // By default we start in read mode.
    // So fetch the first page if the file has data.
    if (actualFile->GetLength() > 0) {
        actualFile->GetPage(currentPage, 0);
    }
    cout << "Number of pages = " << actualFile->GetLength() << "\n";
    if(createFile){
        // Create a meta file to store the type and name of the file
        std::ofstream outfile (string(f_path) + ".meta");
        outfile << "heap" << std::endl;
        outfile.close();
    }
}

int HeapFile::Create(const char *f_path, fType f_type, void *startup)
{
    cout << "create called \n";
    // Assumption: if file already exists, it would be over written.
    initState(true, f_path);
    return 1;
}

void HeapFile::Load(Schema &f_schema, const char *loadpath) {
    cout << "Load called. \n";
    if (!fileExists(loadpath)) return;
    FILE *tableFile = fopen(loadpath, "r");
    ComparisonEngine comp;
    Record temp;
    int count = 0;
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1) {
       Add(temp);
       count++;
    }
    cout << "Added " << count << " records !!!!! \n";
}

int HeapFile::Open(const char *f_path) {
    cout << "Open called \n";
    // Return failure if we can't find the file at
    // the given path.
    if (!fileExists(f_path)) return 0;
    initState(false, f_path);
    return 1;
}

void HeapFile::MoveFirst() {
    // cout << "MoveFirst called \n";
    if (!inReadMode) {
        // Write dirty changes back to disk.
        writePageToDisk(currentPage);
        currentPage->EmptyItOut();
        inReadMode = true;
    }
    // Move the read indexes back to zero.
    currentReadPageIndex = 0;
    lastReturnedRecordIndex = -1;

    updatePageToLocation(currentPage, currentReadPageIndex, lastReturnedRecordIndex);
}

int HeapFile::Close() {
    cout << "Close called.\n";
    // If file or page is null, we have already closed this file.
    if (actualFile == NULL || currentPage == NULL) return 0;

    if (!inReadMode) {
        // We have un-written data.
        writePageToDisk(currentPage);
        currentPage->EmptyItOut();
        inReadMode = true;
    }
    actualFile->Close();
    delete actualFile;
    actualFile = NULL;

    delete currentPage;
    currentPage = NULL;
    return 1;
}

// Warning: Empties out the page passed-in.
void HeapFile::updateToLastPage(Page *page) {
    // Get rid of current data.
    page->EmptyItOut();
    // Get the last page.
    int length = actualFile->GetLength();
    if (length > 0) {
        actualFile->GetPage(page, length - 2);
    }
}

void HeapFile::Add(Record &rec) {
    // cout << "Add called \n";
    // If we are in read mode, switch to write mode and
    // fetch the last page.
    if (inReadMode) {
        // cout << "IN READ MODE \n"; 
        inReadMode = false;
        writeInPlace = true;
        // We won't be losing any data because of this page
        // switch, as dirty data gets written to disk when the mode
        // changed to read last time around.
        updateToLastPage(currentPage);
        Add(rec);
        return;
    }

    // We are in write mode, which means the page we have in memory now
    // is the last page to which we can write.
    // TODO: Should we avoid a page rewrite when the page we get is
    // already full (even before the first call to add).
    if (currentPage->Append(&rec) == 0) {
        // Append failed-> Page is full.
        // Write this current page to disk.
        writePageToDisk(currentPage);
        currentPage->EmptyItOut();
        Add(rec);
        return;
    }
}

void HeapFile::writePageToDisk(Page *page) {
    // cout << "write page called \n";
    int length = actualFile->GetLength();
    int writeLocation = writeInPlace ? length - 2 : length - 1; 
    int offset = length == 0? 0 : writeLocation;
    actualFile->AddPage(page, offset);
    writeInPlace = false;
}

// Warning: Current data if any will be erased from the page.
void HeapFile::updatePageToLocation(Page *page, int pageIndex, int location) {
    // cout << "updatePageToLocation called. pageIndex = " << pageIndex << "\n";
    // Get rid of current data.
    page->EmptyItOut();
    int length = actualFile->GetLength();
    if (length == 0) return;
    // Get the page at the given index.
    actualFile->GetPage(page, pageIndex);
    // Pop elements till we are at the given location.
    int index = -1;
    Record temp;
    while (index != location) {
        page->GetFirst(&temp);
        index++;
    }
}

int HeapFile::GetNext(Record &fetchme) {
    if (inReadMode == false) {
        // We are in write mode.
        // We may have dirty data which is not yet written to disk.
        writePageToDisk(currentPage);
        inReadMode = true;
        // Get the page that corresponds to last read location.
        updatePageToLocation(currentPage, currentReadPageIndex, lastReturnedRecordIndex);
        return GetNext(fetchme);
    }
    // We are in read mode - which means we can just get 
    // the first record from the current page.
    if (currentPage->GetFirst(&fetchme) == 0) {
        // Read failed -> page is empty.
        // If this was the last page of the file, we don't have 
        // any more records. Otherwise just move to next page
        // and call this method again.
        int length = actualFile->GetLength();
        if (currentReadPageIndex + 2 >= length) return 0;
        lastReturnedRecordIndex = -1;
        updatePageToLocation(currentPage, ++currentReadPageIndex, lastReturnedRecordIndex);
        return GetNext(fetchme);
    }
    lastReturnedRecordIndex++;
    
    return 1;
}

int HeapFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    while (GetNext(fetchme) == 1) {
        if (comp.Compare (&fetchme, &literal, &cnf)) {
            return 1;
        }
    }
    return 0;
}
