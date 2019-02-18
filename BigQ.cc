#include "BigQ.h"
#include "ComparisonEngine.h"


using namespace std;

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

struct HeapRecord
{
	OrderMaker *orderMaker;
	Record record;
	int currentPageIndex;
	int lastPageIndex;
	int currentRecordIndex;

	bool operator<(const HeapRecord &right) const
	{
		ComparisonEngine comp;
		return comp.Compare((Record *)&record, (Record *)&right.record, orderMaker) < 0;
	}
};

void BigQ::writeLastPageToFile(Page *page)
{
	int length = heapFile->GetLength();
	int writeLocation = length - 1;
	int offset = length == 0 ? 0 : writeLocation;
	heapFile->AddPage(page, offset);
}

// Sort records in the array.
// Write till page exhausts.
// Write that page to file.
// Move to next page.
// Record first page and length.
struct HeapRecord BigQ::writeRunOfRecords(vector<Record> records, int recordCount, OrderMaker *sortorder)
{
	// printf("writeRunOfRecords called. recordCount = %d \n", recordCount);
	sort(records.begin(), records.end(), RecordComparator(sortorder));

	int length = heapFile->GetLength();
	int firstPageIndex = length == 0 ? 0 : length - 1;
	int lastPageIndex = firstPageIndex;

	std::vector<Record>::iterator recordIterator = records.begin();
	Page currentPage;
	Record firstRecord = *recordIterator;

	while (recordIterator != records.end())
	{
		Record record = *recordIterator;
		if (currentPage.Append(&record) == 0)
		{
			// Write page to file.
			writeLastPageToFile(&currentPage);
			// Empty current page.
			currentPage.EmptyItOut();
			// Append again.
			currentPage.Append(&record);
			lastPageIndex++;
		}
		++recordIterator;
	}
	// Write remaining data
	writeLastPageToFile(&currentPage);

	// printf("firstPageIndex =  %d \n", firstPageIndex);
	// printf("lastPageIndex =  %d \n", lastPageIndex);
	HeapRecord heapRecord;
	heapRecord.orderMaker = sortorder;
	heapRecord.record = firstRecord;
	heapRecord.currentPageIndex = firstPageIndex;
	heapRecord.currentRecordIndex = 0;
	heapRecord.lastPageIndex = lastPageIndex;
	return heapRecord;
}

int BigQ::nextHeapRecord(HeapRecord current, HeapRecord *nextRecord)
{
	Page page;
	heapFile->GetPage(&page, current.currentPageIndex);
	int index = -1;
	Record temp;
	bool endOfPage = false;
	while (index != current.currentRecordIndex + 1)
	{
		if (page.GetFirst(&temp) == 0)
		{
			endOfPage = true;
			break;
		}
		index++;
	}
	if (endOfPage == false)
	{
		HeapRecord next;
		next.record = temp;
		next.currentPageIndex = current.currentPageIndex;
		next.currentRecordIndex = index;
		next.lastPageIndex = current.lastPageIndex;
		*nextRecord = next;
		return 1;
	}
	// We have exhausted a page.
	if (current.currentPageIndex == current.lastPageIndex)
	{
		// Return failure if we were on the last page.
		return 0;
	}
	// Move over to next page.
	heapFile->GetPage(&page, current.currentPageIndex + 1);
	page.GetFirst(&temp);
	HeapRecord next;
	next.record = temp;
	next.currentPageIndex = current.currentPageIndex + 1;
	next.currentRecordIndex = 0;
	next.lastPageIndex = current.lastPageIndex;
	*nextRecord = next;
	return 1;
}

void BigQ::startMerge() {
	// printf("In the startMerge method \n");
	vector<Record> records;
	Record record;
	Page page;
	int pageCount = 0;
	int recordCount = 0;
	int numberOfRuns = 0;
	vector<HeapRecord> runs;
	// printf("Will call remove \n");
	while (inputPipe->Remove(&record) == 1)
	{
		// Copy the record as the Page append API consumes it.
		Record copied;
		copied.Copy(&record);
		// First try to append the record to the current page.
		if (page.Append(&record) == 0)
		{
			// Append failed.
			page.EmptyItOut();
			// Do we have enough records for a run?
			if (pageCount >= runLength)
			{
				// We have filled data worth one run length.
				// printf("Will write one run. Number of records in run = %d \n", recordCount);
				pageCount = 0;
				runs.push_back(writeRunOfRecords(records, recordCount, orderMaker));
				recordCount = 0;
				// recordCount = 0;
				numberOfRuns++;
				records.clear();
			}
			else
			{
				// We have exhausted a page.
				pageCount++;
			}
			// Append current record to the emptied out page.
			page.Append(&record);
		}
		records.push_back(copied);
		recordCount++;
	}
	runs.push_back(writeRunOfRecords(records, recordCount, orderMaker));

	printf("Number of runs = %d \n", numberOfRuns);
	// printf("Number of heap records = %d \n", runs.size());
	// printf("Number of records = %d \n", recordCount);
	// printf("Records array size = %lu \n", records.size());

	// Construct a priority queue.
	priority_queue<HeapRecord> mergeQueue;
	for (std::vector<HeapRecord>::iterator runsIterator = runs.begin(); runsIterator != runs.end(); ++runsIterator)
	{
		HeapRecord heapRecord = *runsIterator;
		mergeQueue.push(heapRecord);
	}

	int recordsWritten = 0;
	while (mergeQueue.size() > 0)
	{
		HeapRecord topRecord = mergeQueue.top();
		mergeQueue.pop();
		HeapRecord nextRecord;
		if (nextHeapRecord(topRecord, &nextRecord) == 1)
		{
			nextRecord.orderMaker = orderMaker;
			mergeQueue.push(nextRecord);
		}
		// Push the record to output pipe.
		outputPipe->Insert(&topRecord.record);
		recordsWritten++;
	}
	printf("Number of records written to pipe = %d \n", recordsWritten);
	outputPipe->ShutDown();
}

void* run(void *arg)
 {
	BigQ *bigQ = (BigQ *)arg;
	bigQ->startMerge();
    pthread_exit(NULL);
 }

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
	// cout << "Biq constructor called \n";
	inputPipe = &in;
	outputPipe = &out;
	orderMaker = &sortorder;
	runLength = runlen;
	heapFile = new File();
	heapFile->Open(0, "heap_file_sort_file.bin");

	pthread_t bigThread;
	pthread_create(&bigThread, NULL, &run, (void *)this);
}

BigQ::~BigQ()
{
	// printf("Destruct called. \n");
	if (heapFile != NULL)
	{
		heapFile->Close();
		delete heapFile;
		heapFile = NULL;
	}
}
