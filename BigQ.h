#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"

class BigQ {

private:
	File *heapFile;
	void writeLastPageToFile(Page *page);
	struct HeapRecord  writeRunOfRecords(vector<Record> records, int recordCount, OrderMaker &sortorder);
	int nextHeapRecord(HeapRecord current, HeapRecord *heapRecord);
	bool comparator(const HeapRecord &left, const HeapRecord &right);

public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
};

#endif
