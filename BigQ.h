#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <vector>
#include <algorithm>
#include <queue>

class BigQ {

private:
	Pipe *inputPipe;
	File *heapFile;
	int runLength;
	OrderMaker *orderMaker;
	Pipe *outputPipe;
	void writeLastPageToFile(Page *page);
	struct HeapRecord  writeRunOfRecords(vector<Record> records, int recordCount, OrderMaker *sortorder);
	int nextHeapRecord(HeapRecord current, HeapRecord *heapRecord);
	bool comparator(const HeapRecord &left, const HeapRecord &right);

public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	~BigQ ();
	void startMerge();
};

#endif
