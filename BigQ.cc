#include "BigQ.h"
#include <vector>
#include "ComparisonEngine.h"

struct RecordComparator {
	OrderMaker *orderMaker;
	RecordComparator(OrderMaker *order) : orderMaker(order) {}

	bool operator()(Record &left, Record &right) const
	{
		ComparisonEngine comp;
		return comp.Compare(&left, &right, orderMaker);
	}
};

BigQ ::BigQ(Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen)
{
	cout << "Biq constructor called \n";

	// vector<Record> records = recordsFromInputPipe(in, runlen);

	vector<Record> records;
	Record record;
	Page page;
	int pageCount = 0;
	int recordCount = 0;
	int numberOfRuns = 0;
	while (in.Remove (&record) == 1) {
		// Copy the record as the Page append API consumes it.
		Record copied;
		copied.Copy(&record);
		// First try to append the record to the current page.
		if (page.Append(&record) == 0) {
			// Append failed.
			page.EmptyItOut();
			// Do we have enough records for a run?
			if (pageCount >= runlen) {
				// We have filled data worth one run length.
				printf("Will write one run. Number of records in run = %d \n", recordCount);
				// Sort records in the array.
				// Create pages
				// Write them to File.
				// Empty out the vector
				pageCount = 0;
				writeRunOfRecords(records, recordCount, sortorder);
				recordCount = 0;
				// recordCount = 0;
				numberOfRuns++;
				records.clear();
			}
			else {
				// We have exhausted a page.
				pageCount++;
			}
			// Append current record to the emptied out page.
			page.Append(&record);
		}
		records.push_back(copied);
		recordCount++;
	}
	writeRunOfRecords(records, recordCount, sortorder);

	printf("Number of runs = %d \n", numberOfRuns);
	printf("Number of records = %d \n", recordCount);
	printf("Records array size = %lu \n", records.size());
	// cout << "records size = " << records.size() << "\n";

	// Get run length worth of records (or smaller if the pipe exhausts)
	// Sort them
	// Create pages from the sorted records.
	// Write the pages to the file.

	// printf("Read %d records \n", recordCount);
	// cout << "Read  %d \n", records. \n";

	// read data from in pipe sort them into runlen pages

    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

void BigQ::writeRunOfRecords(vector<Record> records, int recordCount, OrderMaker &sortorder) {
	printf("writeRunOfRecords called. recordCount = %d \n", recordCount);
	sort(records.begin(), records.end(), RecordComparator(&sortorder));

	for (std::vector<Record>::iterator it=records.begin(); it!=records.end(); ++it) {
		Record record = *it;
		record.Print(new Schema ("catalog", "region"));
	}
}

BigQ::~BigQ () {
}
