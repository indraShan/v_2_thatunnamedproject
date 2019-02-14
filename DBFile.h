#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class File;
class Page;

typedef enum {heap, sorted, tree} fType;

// stub DBFile header..replace it with your own DBFile.h 

class DBFile {

private:
	bool writeInPlace;
	File *actualFile;
	Page *currentPage;
	int currentReadPageIndex;
	int lastReturnedRecordIndex;
	fType fileType;
	bool inReadMode;
	void initState(bool createFile, const char *f_path);
	void updateToLastPage(Page *page);
	bool fileExists(const char *f_path);
	void writePageToDisk(Page *page);
	void updatePageToLocation(Page *page, int pageIndex, int location);

public:
	DBFile (); 
	virtual ~DBFile ();

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
