#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include "DBFile.h"
#include "BigQ.h"

class SortedDBFile : public GenericDBFile {

private:
	File *actualFile;
    File *tempFile;
    OrderMaker *fileOrderMaker;
    int runLength;
    BigQ *bigQueue;
    OrderMaker *queryOrderMaker;
    Pipe *inputPipe;
    Pipe *outputPipe;
    char *filePath;
    // True when the file is is read mode.
    bool inReadMode;
    int currentReadPageIndex;
	int lastReturnedRecordIndex;
    Page *currentReadPage;
    bool fileExists(const char *f_path);
    void initState(bool createFile, const char *f_path, OrderMaker *order, int runLength);
    void initializeBigQueue();
    void addRecord(Record &addme);
    void writePipeRecordsToFile();
    void switchToReadMode();
    void createTempFile();
    int getNextPipeRecord(Record *record);
    int getNextFileRecord(Record *record);
    void moveReadPageToFirstRecord();
    void updatePageToLocation(Page *page, int pageIndex, int location);

public:
    SortedDBFile (); 
    virtual ~SortedDBFile();

    int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};
#endif