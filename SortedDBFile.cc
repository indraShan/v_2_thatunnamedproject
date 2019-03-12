#include "SortedDBFile.h"
#include <iostream>
#include "Comparison.h"
#include <cstring>
#include <map>
#include <fstream>
#include <sstream>

struct SortOrder
{
    OrderMaker *orderMaker;
    int runLength;
};

char *createCharArrayByCopying(char *array)
{
    size_t length = strlen(array);
    char *result = new char[length];
    strcpy(result, array);
    return result;
}

char *createCharByAppending(char *array, char *append)
{
    char *result = new char[std::strlen(array) + std::strlen(append) + 1];
    std::strcpy(result, array);
    std::strcat(result, append);
    return result;
}

// Returns true if the file exists at given path.
// false otherwise.
bool SortedDBFile::fileExists(const char *f_path)
{
    struct stat buf;
    return (stat(f_path, &buf) == 0);
}

SortedDBFile ::~SortedDBFile()
{
    cout << "SortedDBFile being destroyed\n";
    Close();
}

SortedDBFile::SortedDBFile()
{
    actualFile = NULL;
    currentReadPage = NULL;
    fileOrderMaker = NULL;
    queryOrderMaker = NULL;
    bigQueue = NULL;
    inputPipe = NULL;
    outputPipe = NULL;
    tempFile = NULL;
    filePath = NULL;
    tempFilePath = NULL;
}

void SortedDBFile::moveReadPageToFirstRecord()
{
    currentReadPage->EmptyItOut();
    if (actualFile->GetLength() > 0)
    {
        actualFile->GetPage(currentReadPage, 0);
    }
    currentReadPageIndex = 0;
    lastReturnedRecordIndex = -1;
}

void SortedDBFile::initState(bool createFile, const char *f_path, OrderMaker *order, int runLength)
{
    actualFile = new File();
    currentWritePage = new Page();
    currentReadPage = new Page();
    currentReadPageIndex = 0;
    lastReturnedRecordIndex = -1;
    this->fileOrderMaker = order;
    this->runLength = runLength;
    this->filePath = (char *)f_path;
    this->tempFilePath = createCharByAppending(filePath, (char *)".temp.bin");
    // By default we start off in read mode.
    inReadMode = true;

    // Create or open the file.
    actualFile->Open(createFile == true ? 0 : 1, f_path);
    moveReadPageToFirstRecord();
    cout << "Number of pages = " << actualFile->GetLength() << "\n";
    if (createFile){
        // Create a meta file to store the type and name of the file
        std::ofstream outfile (string(f_path) + ".meta");
        outfile << "sorted" << std::endl;
        outfile << runLength << std::endl;
        outfile << order->toString() << std::endl;
        outfile.close();
    }
}

// Create the metadata file in the same folder where
// the binary file will be created
// Write file type
// and the run length and order maker to the meta file.
int SortedDBFile::Create(const char *f_path, fType f_type, void *startup)
{
    cout << "create called \n";
    SortOrder *sortOrder = ((SortOrder *)startup);
    // printf("run length = %d \n", sortOrder->runLength);
    // Assumption: if file already exists, it would be over written.
    // TODO: Order maker should allocated again here.
    // TODO: Pass the actual order maker and run length
    initState(true, createCharArrayByCopying((char *)f_path),
              sortOrder->orderMaker, sortOrder->runLength);
    return 1;
}

// Read run length and contruct order maker from the
// meta file.
int SortedDBFile::Open(const char *f_path)
{
    cout << "Open called \n";
    if (!fileExists(f_path))
        return 0;

    std::string f_type, runlength, line, numAtts;
    std::ifstream file(string(f_path)+".meta");
    getline(file,f_type);
    getline(file,runlength);
    map<string,Type> typeMap;
    typeMap["String"] =  String;
    typeMap["Int"] =  Int;
    typeMap["Double"] =  Double;
    // Get the number of attributes in orderMaker
    std::getline(file,numAtts);
    int nAtts = std::stoi(numAtts);
    OrderMaker *o = new OrderMaker();
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
        o->testing_helper_setAttributes(nAtts, attributes, types);
    }
    initState(false, f_path, o, stoi(runlength));
    return 1;
}

void SortedDBFile::Load(Schema &f_schema, const char *loadpath)
{
    printf("Load called. \n");
    if (!fileExists(loadpath))
        return;
    FILE *tableFile = fopen(loadpath, "r");
    Record temp;
    int count = 0;
    while (temp.SuckNextRecord(&f_schema, tableFile) == 1)
    {
        Add(temp);
        count++;
    }
    printf("Added %d records !!!!! \n", count);
}

void SortedDBFile::initializeBigQueue()
{
    // TODO: Figure out the correct buffer size.
    inputPipe = new Pipe(100);
    outputPipe = new Pipe(100);
    // Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen
    bigQueue = new BigQ(*inputPipe, *outputPipe, *fileOrderMaker, runLength);
}

// Adds a record to input pipe
void SortedDBFile::addRecord(Record &rec)
{
    inputPipe->Insert(&rec);
}

void SortedDBFile::switchToReadMode()
{
    printf("switchToReadMode called \n");
    // Write sorted records to file.
    writePipeRecordsToFile();
    // Go back to read mode.
    inReadMode = true;

    // Clean up as BigQ would be created again for the next
    // mode switch.
    // TODO: Uncomment. test.
    // delete inputPipe;
    // inputPipe = NULL;

    // TODO: Uncommenting this crashes due to double release?
    // outputPipe->ShutDown();
    // delete outputPipe;
    // outputPipe = NULL;

    delete bigQueue;
    bigQueue = NULL;
}

void SortedDBFile::createTempFile()
{
    // TODO: Probable leak for char *?
    tempFile = new File();
    tempFile->Open(0, tempFilePath);
}

int SortedDBFile::getNextPipeRecord(Record *record)
{
    return outputPipe->Remove(record);
}

int SortedDBFile::readNextFileRecord(Record *record)
{
    if (currentReadPage->GetFirst(record) == 0)
    {
        // Read failed -> current page is empty.
        // If this was the last page of the file, we don't have
        // any more records. Otherwise just move to next page
        // and call this method again.
        int length = actualFile->GetLength();
        if (currentReadPageIndex + 2 >= length)
            return 0;
        lastReturnedRecordIndex = -1;
        updatePageToLocation(currentReadPage, ++currentReadPageIndex, lastReturnedRecordIndex);
        return readNextFileRecord(record);
    }
    lastReturnedRecordIndex++;
    return 1;
}

// Warning: Current data if any will be erased from the page.
void SortedDBFile::updatePageToLocation(Page *page, int pageIndex, int location)
{
    // cout << "updatePageToLocation called. pageIndex = " << pageIndex << "\n";
    // Get rid of current data.
    page->EmptyItOut();
    int length = actualFile->GetLength();
    if (length == 0)
        return;
    // Get the page at the given index.
    actualFile->GetPage(page, pageIndex);
    // Pop elements till we are at the given location.
    int index = -1;
    Record temp;
    while (index != location)
    {
        page->GetFirst(&temp);
        index++;
    }
}

void SortedDBFile::writeSortedPageToTempFile() {
    int length = tempFile->GetLength();
    int offset = length == 0? 0 : length - 1;
    tempFile->AddPage(currentWritePage, offset);
}

// Writes the given record to the temp file.
void SortedDBFile::writeSortedRecordToTempFile(Record record) {
    if (currentWritePage->Append(&record) == 0) {
        // Append failed-> Page is full.
        // Write this current page to disk.
        writeSortedPageToTempFile();
        currentWritePage->EmptyItOut();
        writeSortedRecordToTempFile(record);
        return;
    }
}

// -> Merge BigQ with File. Mode = reading.
//      new file instance.
//      Shutdown inputpipe.
//      Output remove in loop
//      Current file current page
//      Compare write to new instance file.
//      updates new files name in metadata.
// free bigQ
// bigQ = nil
// Reset reset pointer to correct location in file.
void SortedDBFile::writePipeRecordsToFile()
{
    printf("writePipeRecordsToFile called \n");
    createTempFile();
    inputPipe->ShutDown();
    // Reset current read page to be at the first record of the file.
    moveReadPageToFirstRecord();

    Record pipeRecord;
    Record fileRecord;

    // Get first records from both sources.
    int pipe = getNextPipeRecord(&pipeRecord);
    int file = readNextFileRecord(&fileRecord);

    currentWritePage->EmptyItOut();
    ComparisonEngine comp;
    // Compare records from the output pipe, with records in the
    // current file.
    do
    {
        if (pipe != 0 && file != 0)
        {
            // Compare the two, write one to temp file.
            // Progress the source from which the record was choosen.
            if (comp.Compare(&pipeRecord, &fileRecord, fileOrderMaker) < 0)
            {
                // pipeRecord is smaller
                // Write pipe record to temp file.
                writeSortedRecordToTempFile(pipeRecord);
                pipe = getNextPipeRecord(&pipeRecord);
            }
            else
            {
                // fileRecord is smaller
                // Write fileRecord record to temp file.
                writeSortedRecordToTempFile(fileRecord);
                file = readNextFileRecord(&fileRecord);
            }
        }
        else if (pipe != 0)
        {
            // Write pipe record. Progress
            writeSortedRecordToTempFile(pipeRecord);
            pipe = getNextPipeRecord(&pipeRecord);
        }
        else
        {
            // Write fileRecord record to temp file.
            writeSortedRecordToTempFile(fileRecord);
            file = readNextFileRecord(&fileRecord);
        }
    } while (pipe != 0 || file != 0);
    printf("After loop \n");
    writeSortedPageToTempFile();

    // Close current file.
    // Delete pointer.
    // TODO: Delete file.
    actualFile->Close();
    delete actualFile;
    actualFile = NULL;
    remove(filePath);
    
    // Close temp file
    // Delete pointer.
    // TODO: Rename file.
    tempFile->Close();
    delete tempFile;
    tempFile = NULL;
    rename(tempFilePath, filePath);

    // Reset currentFile pointer to open the renamed file.
    // Go back to last read position
    // Rest current read page to first record or to correct place.
    actualFile = new File();
    actualFile->Open(1, filePath);
    moveReadPageToFirstRecord();
}

// writing -> insert to BigQ  (done)
// reading -> BigQ must be nil. Create BigQ. Insert. Mode = write. (done)
// Mode-> Writing. GetNext, Close, MoveFirst.
void SortedDBFile::Add(Record &rec)
{
    printf("Add called. Sorted DV \n");
    if (inReadMode == true)
    {
        // Create a new instance of BigQ
        initializeBigQueue();
        // Insert record into pipe
        addRecord(rec);
        inReadMode = false;
        return;
    }
    // We are in write mode, lets just keeping adding records to pipe.
    addRecord(rec);
}

int SortedDBFile::GetNext(Record &fetchme)
{
    printf("GetNext called \n");
    if (inReadMode == false)
    {
        switchToReadMode();
        return GetNext(fetchme);
    }
    return readNextFileRecord(&fetchme);
}

// Searches the entire page to check if is present in it.
void SortedDBFile::linearSearchPageToFind(Record *literal, Record *record,
                                          int pageIndex, int *resultPage,
                                          int *recordIndexInPage)
{
    Page page;
    actualFile->GetPage(&page, pageIndex);
    // The page doesnt have data?
    if (page.GetFirst(record) == 0)
    {
        *resultPage = -1;
        *recordIndexInPage = -1;
        return;
    }
    ComparisonEngine comp;
    int result = comp.Compare(literal, record, queryOrderMaker);
    int recordIndex = 0; 
    while (result < 0)
    {
        if (page.GetFirst(record) == 0)
        {
            break;
        }
        recordIndex++;
        result = comp.Compare(literal, record, queryOrderMaker);
    }
    if (result == 0)
    {
        *resultPage = pageIndex;
        *recordIndexInPage = recordIndex;
        return;
    }
    *resultPage = -1;
    *recordIndexInPage = -1;
}

// Binary searches the file starting at current record
// to find a record that equals the literal for current queryOrderMaker
void SortedDBFile::binarySearchFileToFind(Record *literal, Record *record,
                                          int start, int end,
                                          int *resultPage, int *recordIndexInPage)
{
    // printf("binarySearchFileToFind called \n");
    if (start > end)
    {
        *resultPage = -1;
        *recordIndexInPage = -1;
        return;
    }
    if (start == end)
    {
        linearSearchPageToFind(literal, record, start, resultPage, recordIndexInPage);
        return;
    }
    int mid = start + (end - start) / 2;

    // Get the page at mid index.
    Page page;
    actualFile->GetPage(&page, mid);

    // The page doesnt have data?
    if (page.GetFirst(record) == 0)
    {
        *resultPage = -1;
        *recordIndexInPage = -1;
        return;
    }
    ComparisonEngine comp;
    int result = comp.Compare(literal, record, queryOrderMaker);

    // If temp is equal to literal, return success.
    if (result == 0)
    {
        *resultPage = mid;
        *recordIndexInPage = 0;
        return;
    }
    else if (result < 0)
    {
        // Literal falls to the left of record.
        binarySearchFileToFind(literal, record, start, mid - 1, resultPage, recordIndexInPage);
    }
    else
    {
        // Its possible that the literal is contained in this page.
        // Check if the literal is lesser than first record of next page.
        page.EmptyItOut();
        actualFile->GetPage(&page, mid+1);
        if (page.GetFirst(record) == 0)
        {   
            *resultPage = -1;
            *recordIndexInPage = -1;
            return;
        }
        ComparisonEngine comp;
        result = comp.Compare(literal, record, queryOrderMaker);
        if (result < 0) {
            // Literal is contained within current page.
            linearSearchPageToFind(literal, record, mid, resultPage, recordIndexInPage);
        }
        else {
            // Literal falls right of current mid page.
            binarySearchFileToFind(literal, record, mid+1, end, resultPage, recordIndexInPage);
        }
    }
}

// TODO: Make sure indexes reset to correct place in all cases.
// you look to see if the attribute is present in any of the
// subexpressions (disjunctions) in the CNF instance that you
// are asked to search on. If this attribute is in the CNF instance,
// and it is the only attribute present in its subexpression, and
// the CNF instance says that it is comparing that attribute with
//  a literal value with an equality check,
// then you add it to the end of the “query” OrderMaker that you are constructing
int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    printf("GetNext with CNF called\n");
    if (inReadMode == false)
    {
        switchToReadMode();
        return GetNext(fetchme, cnf, literal);
    }

    queryOrderMaker = cnf.constructQuerySortOrderFromFileOrder(fileOrderMaker);
    // TODO: Verify if this scenario is correct
    if (queryOrderMaker == NULL) {
        printf("No matching sort order!! \n");
        // Return first record of the file.
        if (currentReadPageIndex != 0 || lastReturnedRecordIndex != -1) {
            MoveFirst();
        }
        return readNextFileRecord(&fetchme);
    }
    queryOrderMaker->Print();
    Record temp;
    int startPageIndex = currentReadPageIndex;
    int fileLength = actualFile->GetLength();
    int endPageIndex = fileLength > 0 ? fileLength - 2 : 0;
    int resultPage = -1;
    int recordIndex = -1;
    binarySearchFileToFind(&literal, &temp, startPageIndex, endPageIndex, &resultPage, &recordIndex);
    if (resultPage == -1) return 0;
    currentReadPageIndex = resultPage;
    lastReturnedRecordIndex = recordIndex;
    updatePageToLocation(currentReadPage, currentReadPageIndex, lastReturnedRecordIndex);

    ComparisonEngine comp;
    do {
        if (comp.Compare(&literal, &temp, queryOrderMaker) != 0) return 0;
        if (comp.Compare(&literal, &temp, &cnf) == 0) {
            return 1;
        }
    } while(readNextFileRecord(&temp) != 0);
    
    return 0;
}

void SortedDBFile::MoveFirst()
{
    if (inReadMode == false)
    {
        switchToReadMode();
        return;
    }
    moveReadPageToFirstRecord();
}

// TODO: Dealloc everything
int SortedDBFile::Close()
{
    cout << "Close called.\n";
    if (inReadMode == false)
    {
        switchToReadMode();
    }
    // If file or page is null, we have already closed this file.
    if (actualFile == NULL)
        return 0;

    actualFile->Close();
    delete actualFile;
    actualFile = NULL;

    delete currentReadPage;
    currentReadPage = NULL;

    delete currentWritePage;
    currentWritePage = NULL;

    if (tempFile != NULL) {
        tempFile->Close();
        delete tempFile;
        tempFile = NULL;
    }
 
    // TODO: Why this crashes? leak?
    // delete filePath;
    // delete tempFilePath;

    return 1;
}