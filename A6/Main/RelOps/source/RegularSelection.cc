
#ifndef REG_SELECTION_C                                        
#define REG_SELECTION_C

#include "RegularSelection.h"

void RegularSelection :: run () {
    MyDB_RecordIteratorAltPtr iterator = this->input->getIteratorAlt();
    MyDB_RecordPtr inRecord = this->input->getEmptyRecord();
    MyDB_RecordPtr outRecord = this->output->getEmptyRecord();

    vector<func> computations;
    for (auto& predicate : this->projections)
        computations.push_back(inRecord->compileComputation(predicate));

    func selectionPredicate = inRecord->compileComputation(this->selectionPredicate);

    while (iterator->advance()) {
        iterator->getCurrent(inRecord);

        if (selectionPredicate()->toBool()) {
            int i = 0;
            for (auto& computation : computations)
                outRecord->getAtt(i++)->set(computation());

            outRecord->recordContentHasChanged();

            this->output->append(outRecord);
        }
    }
}

#endif
