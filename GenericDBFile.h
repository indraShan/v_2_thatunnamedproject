#ifndef GENDBFILE_H
#define GENDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

class File;
class Page;

typedef enum {heap, sorted, tree} fType;


class GenericDBFile {

public:
	virtual GenericDBFile (); 
	virtual ~GenericDBFile ();

	virtual int Create (const char *fpath, fType file_type, void *startup);
	virtual int Open (const char *fpath);
	virtual int Close ();

	virtual void Load (Schema &myschema, const char *loadpath);

	virtual void MoveFirst ();
	virtual void Add (Record &addme);
	virtual int GetNext (Record &fetchme);
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};

#endif
