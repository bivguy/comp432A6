
#ifndef BPLUS_SELECTION_C                                        
#define BPLUS_SELECTION_C

#include "BPlusSelection.h"

BPlusSelection :: BPlusSelection (MyDB_BPlusTreeReaderWriterPtr input, MyDB_TableReaderWriterPtr output,
                MyDB_AttValPtr low, MyDB_AttValPtr high,
                string selectionPredicate, vector <string> projections) 
                {
                    this->input = input;
                    this->output = output;
                    this->low = low;
                    this->high = high;
                    this->selectionPredicate = selectionPredicate;
                    this->projections = projections;
                }

void BPlusSelection :: run () {
    // get the input record
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();

    // get all the records in the specified range
    MyDB_RecordIteratorAltPtr iter = input->getRangeIteratorAlt(low, high);

    // get the predicate
    func pred = inputRec->compileComputation (selectionPredicate);

    vector<func> computations;
    for (auto& projection : this->projections)
        computations.push_back(inputRec->compileComputation(projection));

    MyDB_RecordPtr outputRec = output->getEmptyRecord();

    // go through all of the records
    while (iter->advance ()) {
        // load the current record
        iter->getCurrent (inputRec);

        // see if it is accepted by the predicate
		if (!pred()->toBool ()) {
			continue;
		}

        int i = 0;
        for (auto& computation : computations)
            outputRec->getAtt(i++)->set(computation());

        outputRec->recordContentHasChanged();

        // now add this record to the output
        output->append(outputRec);
    }
}

#endif
