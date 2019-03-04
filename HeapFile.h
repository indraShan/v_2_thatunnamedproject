#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "GenericDBFile.h"
#include "Comparison.h"
#include "ComparisonEngine.h"


class File;
class Page;

class HeapFile : public GenericDBFile{
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
		HeapFile();
		virtual ~HeapFile();

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
