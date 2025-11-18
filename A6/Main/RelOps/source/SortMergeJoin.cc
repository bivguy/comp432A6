
#ifndef SORTMERGE_CC
#define SORTMERGE_CC

#include "Aggregate.h"
#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "SortMergeJoin.h"
#include "Sorting.h"

SortMergeJoin :: SortMergeJoin (MyDB_TableReaderWriterPtr leftInput, MyDB_TableReaderWriterPtr rightInput,
                MyDB_TableReaderWriterPtr output, string finalSelectionPredicate, 
                vector <string> projections,
                pair <string, string> equalityCheck, string leftSelectionPredicate,
                string rightSelectionPredicate) {
                    this->leftTable = leftInput;
                    this->rightTable = rightInput;
                    this->output = output;

                    this->finalSelectionPredicate = finalSelectionPredicate;
                    this->projections = projections;
                    this->equalityCheck = equalityCheck;
                    this->leftSelectionPredicate = leftSelectionPredicate;
                    this->rightSelectionPredicate = rightSelectionPredicate;
                }

void SortMergeJoin :: run () {
    // Sort the left table
    MyDB_RecordPtr leftRecord = this->leftTable->getEmptyRecord();
    MyDB_RecordPtr rightRecord = this->leftTable->getEmptyRecord();
    function <bool ()> comparator = buildRecordComparator(leftRecord, rightRecord, this->equalityCheck.first);
    MyDB_RecordIteratorAltPtr leftq = buildItertorOverSortedRuns(this->leftTable->getBufferMgr()->numPages / 2, *this->leftTable, comparator, leftRecord, rightRecord, this->leftSelectionPredicate);

    // Sort the right table
    leftRecord = this->rightTable->getEmptyRecord();
    rightRecord = this->rightTable->getEmptyRecord();
    comparator = buildRecordComparator(leftRecord, rightRecord, this->equalityCheck.second);
    MyDB_RecordIteratorAltPtr rightq = buildItertorOverSortedRuns(this->rightTable->getBufferMgr()->numPages / 2, *this->rightTable, comparator, leftRecord, rightRecord, this->rightSelectionPredicate);

    leftRecord = this->leftTable->getEmptyRecord();
    rightRecord = this->rightTable->getEmptyRecord();

    MyDB_SchemaPtr combinedSchema = make_shared<MyDB_Schema>();
    for (auto &p : this->leftTable->getTable ()->getSchema ()->getAtts ())
		combinedSchema->appendAtt (p);
	for (auto &p : this->rightTable->getTable ()->getSchema ()->getAtts ())
		combinedSchema->appendAtt (p);

    MyDB_RecordPtr combinedRecord = make_shared<MyDB_Record>(combinedSchema);
    combinedRecord->buildFrom(leftRecord, rightRecord);

    MyDB_RecordPtr outputRecord = this->output->getEmptyRecord();

    vector <func> finalComputations;
    for (auto &projection : this->projections) {
        finalComputations.push_back(combinedRecord->compileComputation(projection));
    }

    func finalPredicate = combinedRecord->compileComputation(this->finalSelectionPredicate);

    MyDB_RecordPtr tempRecord = this->leftTable->getEmptyRecord();
    if (leftq->advance() && rightq->advance()) {
        leftq->getCurrent(tempRecord);
        rightq->getCurrent(rightRecord);

        function <bool ()> ltComparator = buildRecordComparatorLt(tempRecord, rightRecord, this->equalityCheck.first, this->equalityCheck.second);
        function <bool ()> eqComparator = buildRecordComparatorEq(tempRecord, rightRecord, this->equalityCheck.first, this->equalityCheck.second);

        while (true) {
            // if leftRecord != rightRecord
            if (!eqComparator()) {
                if (ltComparator())
                    if (leftq->advance())
                        leftq->getCurrent(tempRecord);
                    else
                        break;
                else
                    if (rightq->advance())
                        rightq->getCurrent(rightRecord);
                    else
                        break;
            }
            else {
                vector<MyDB_PageReaderWriter> savedRecords;
                MyDB_RecordPtr savedRecord = this->leftTable->getEmptyRecord();
                leftq->getCurrent(savedRecord);

                MyDB_PageReaderWriter pageRW = MyDB_PageReaderWriter(*this->leftTable->getBufferMgr());
                savedRecords.push_back(pageRW);

                bool leftFinished = false;

                // Iterate through the left queue while left == right
                while (eqComparator()) {
                    if (!pageRW.append(tempRecord)) {
                        pageRW = MyDB_PageReaderWriter(*this->leftTable->getBufferMgr());
                        savedRecords.push_back(pageRW);
                        pageRW.append(tempRecord);
                    }

                    if (leftq->advance())
                        leftq->getCurrent(tempRecord);
                    else {
                        leftFinished = true;
                        break;
                    }
                }
                
                function <bool ()> tempComparator = buildRecordComparatorEq(savedRecord, rightRecord, this->equalityCheck.first, this->equalityCheck.second);

                bool rightFinished = false;
                // Iterate through the right table
                while (tempComparator()) {
                    MyDB_RecordIteratorAltPtr tempIterator = getIteratorAlt(savedRecords);
                    
                    while (tempIterator->advance()) {
                        tempIterator->getCurrent(leftRecord);

                        if (finalPredicate()->toBool()) {
                            int i = 0;
                            for (auto &f : finalComputations) {
                                outputRecord->getAtt(i++)->set(f());
                            }

                            outputRecord->recordContentHasChanged();
                            this->output->append(outputRecord);
                        }
                    }

                    if (rightq->advance())
                        rightq->getCurrent(rightRecord);
                    else {
                        rightFinished = true;
                        break;
                    }
                }

                if (leftFinished || rightFinished)
                    break;
            }
        }
    }
}

#endif
