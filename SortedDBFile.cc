#include "SortedDBFile.h"
#include <iostream>
#include "Comparison.h"
#include <cstring>

struct RecordComparator
{
    OrderMaker *orderMaker;
    RecordComparator(OrderMaker *order) : orderMaker(order) {}

    bool operator()(Record &left, Record &right) const
    {
        ComparisonEngine comp;
        return comp.Compare(&left, &right, orderMaker) < 0;
    }
};

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

// TODO: Remove later.
OrderMaker *fakeOrderMaker()
{
    // TODO: Memory leak on purpose.
    OrderMaker *maker = new OrderMaker();
    int attributes[1] = {4};
    Type types[1] = {String};
    maker->testing_helper_setAttributes(1, attributes, types);
    return maker;
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
    currentReadPage = new Page();
    currentReadPageIndex = 0;
    lastReturnedRecordIndex = -1;
    this->fileOrderMaker = order;
    this->runLength = runLength;
    this->filePath = (char *)f_path;
    // By default we start off in read mode.
    inReadMode = true;

    // Create or open the file.
    actualFile->Open(createFile == true ? 0 : 1, f_path);
    moveReadPageToFirstRecord();
    cout << "Number of pages = " << actualFile->GetLength() << "\n";
}

// Create the metadata file in the same folder where
// the binary file will be created
// Write file type
// and the run length and order maker to the meta file.
int SortedDBFile::Create(const char *f_path, fType f_type, void *startup)
{
    cout << "create called \n";
    SortOrder sortOrder = *((SortOrder *)&startup);
    // Assumption: if file already exists, it would be over written.
    // TODO: Order maker should allocated again here.
    // TODO: Pass the actual order maker and run length
    initState(true, createCharArrayByCopying((char *)f_path),
              sortOrder.orderMaker, sortOrder.runLength);
    return 1;
}

// Read run length and contruct order maker from the
// meta file.
int SortedDBFile::Open(const char *f_path)
{
    cout << "Open called \n";
    if (!fileExists(f_path))
        return 0;
    // TODO: Read ordermaker and run length from the meta data file.
    // TODO: Pass the actual order maker and run length
    initState(false, f_path, fakeOrderMaker(), 1);
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
    char *temp_path = createCharByAppending(filePath, (char *)".temp.bin");
    tempFile = new File();
    tempFile->Open(0, temp_path);
}

int SortedDBFile::getNextPipeRecord(Record *record)
{
    return outputPipe->Remove(record);
}

int SortedDBFile::getNextFileRecord(Record *record)
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
        return getNextFileRecord(record);
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

void SortedDBFile::writePipeRecordsToFile()
{
    createTempFile();
    inputPipe->ShutDown();
    // Reset current read page to be at the first record of the file.
    moveReadPageToFirstRecord();

    Record pipeRecord;
    Record fileRecord;

    // Get first records from both sources.
    int pipe = getNextPipeRecord(&pipeRecord);
    int file = getNextFileRecord(&fileRecord);

    Page currentWritePage;
    currentWritePage.EmptyItOut();
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
                
                pipe = getNextPipeRecord(&pipeRecord);
            }
            else
            {
                // fileRecord is smaller
                // Write fileRecord record to temp file.
                file = getNextFileRecord(&fileRecord);
            }
        }
        else if (pipe != 0)
        {
            // Write pipe record. Progress
            pipe = getNextPipeRecord(&pipeRecord);
        }
        else
        {
            // Write fileRecord record to temp file.
            file = getNextFileRecord(&fileRecord);
        }
    } while (pipe != 0 || file != 0);

    // Close current file.
    // Delete pointer.
    // Delete file.

    // Close temp file
    // Delete pointer.
    // Rename file.

    // Reset currentFile pointer to open the renamed file.
    // Go back to last read position
    // Rest current read page to first record or to correct place.

    tempFile->Close();
    delete tempFile;
    tempFile = NULL;
}

// writing -> insert to BigQ  (done)
// reading -> BigQ must be nil. Create BigQ. Insert. Mode = write. (done)
// Mode-> Writing. GetNext, Close, MoveFirst.
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
    return getNextFileRecord(&fetchme);
}

// you look to see if the attribute is present in any of the
// subexpressions (disjunctions) in the CNF instance that you
// are asked to search on. If this attribute is in the CNF instance,
// and it is the only attribute present in its subexpression, and
// the CNF instance says that it is comparing that attribute with
//  a literal value with an equality check,
// then you add it to the end of the “query” OrderMaker that you are constructing
int SortedDBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
    // Build query order maker from cnf
    //      Go through the attributes of this.orderMaker one by one.
    //      First to last.
    //      For each attribute in this.orderMaker check if the attribute
    //      is present in CNF.
    //          if present add it to then end of Query OrderMaker.
    // If order maker is null, return first record of the file
    // Do a binary search on the file to find a record equal to literal using
    // the query order maker
    // If no record equals the literal for given query, return 0.
    // If found:
    //      Start scanning the file starting with found record
    //      For each record,
    //          check if record equals literal using query order maker - return 0 if not.
    //          then, check if record equals literal using CNF.
    //          if not go to next record -> repeat last two steps
    //          else return the matching record to caller.
    //          return 0 on eof before finding a record.
    return 0;
}

void SortedDBFile::MoveFirst()
{
}

// TODO: Dealloc everything
int SortedDBFile::Close()
{
    cout << "Close called.\n";
    // If file or page is null, we have already closed this file.
    if (actualFile == NULL)
        return 0;

    actualFile->Close();
    delete actualFile;
    actualFile = NULL;

    return 1;
}