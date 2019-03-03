#ifndef SORTEDDBFILE_H
#define SORTEDDBFILE_H

#include "DBFile.h"

class SortedDBFile : public GenericDBFile {

private:
	File *actualFile;

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
