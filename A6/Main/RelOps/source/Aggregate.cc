
#ifndef AGG_CC
#define AGG_CC

#include "MyDB_Record.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableReaderWriter.h"
#include "Aggregate.h"
#include <unordered_map>

using namespace std;

Aggregate :: Aggregate (
    MyDB_TableReaderWriterPtr               input, 
    MyDB_TableReaderWriterPtr               output,
    vector <pair <MyDB_AggType, string>>    aggsToCompute,
    vector <string>                         groupings, 
    string                                  selectionPredicate
) {
        this->input = input;
        this->output = output;
        this->aggsToCompute = aggsToCompute;
        this->groupings = groupings;
        this->selectionPredicate = selectionPredicate;
    }

void Aggregate :: run () {
    unordered_map <size_t, void *> myHash;
    // get all of the pages and pin them
    vector <MyDB_PageReaderWriter> allPages;
    input->getBufferMgr()->getPinnedPage();
    for (int i = 0; i < input->getNumPages (); i++) {
        MyDB_PageReaderWriter pinnedPage = input->getPinned (i);

        if (pinnedPage.getType () == MyDB_PageType :: RegularPage)
        {
            allPages.push_back (input->getPinned (i));
        }
    } 

    // represents the pinned anaonymous pages we add our aggregrate records to
    vector <MyDB_PageReaderWriter> aggPages;
    aggPages.push_back(MyDB_PageReaderWriter(true, *input->getBufferMgr()));

    MyDB_RecordPtr outputRec = output->getEmptyRecord ();
    // create a schema that can store all of the required aggregate and grouping attributes
    MyDB_SchemaPtr aggSchema = make_shared <MyDB_Schema> ();

    for (auto &p : output->getTable ()->getSchema ()->getAtts ())
    {
        aggSchema->appendAtt (p);
    }

     // add an extra COUNT
    pair<string, MyDB_AttTypePtr> countAtt = {"[count]", make_shared <MyDB_StringAttType> () };
    aggSchema->appendAtt(countAtt);
    

    MyDB_RecordPtr aggRecord = make_shared <MyDB_Record> (aggSchema);

    aggSchema->getAtts();
    
    // create input record and iterator
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();
    MyDB_RecordIteratorAltPtr iter = getIteratorAlt (allPages);

    // create the combined record schema
    MyDB_SchemaPtr combinedSchema = make_shared <MyDB_Schema> ();
    for (auto &p : aggSchema->getAtts()) {
        combinedSchema->appendAtt(p);
    }

    for (auto &p : inputRec->getSchema()->getAtts()) {
        combinedSchema->appendAtt(p);
    }

    // create the combined record
    MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> ();
    combinedRec->buildFrom (aggRecord, inputRec);

    // have a function for each grouping clause
    vector <func> groupingFuncs;
    for (size_t i = 0; i < groupings.size(); i++) {
        groupingFuncs.push_back(inputRec->compileComputation(groupings[i]));
    }

    // groupings are always first

    // have a function for each aggregate
    vector <func> aggComps;
    // create the count aggregate first
    for (size_t i = 0; i < aggsToCompute.size(); i++) {
        string aggString;
        // get the aggregate 
        pair<MyDB_AggType, string> agg = aggsToCompute[i];
        // means we take the sum of the old agg + the new input record
        if (agg.first == MyDB_AggType :: sum || agg.first == MyDB_AggType :: avg) {
            aggString = "+ (" + agg.second + ", " + ")";


        }
        // MyDB_AttValPtr aggAttr = aggRecord->getAtt(i);
        // TODO: figure out what the correct string is
        string aggString = agg.second;
        aggComps.push_back(combinedRec->compileComputation(aggString));
    }
    // TODO: add an additional count aggregate
    

    func pred = inputRec->compileComputation (selectionPredicate);

    // go through the input table
    while (iter->advance()) {
        iter->getCurrent(inputRec);
        // skip the ones not accepted by the predicate
        if (!pred()->toBool()) {
            continue;
        }

        // hash the current record 
        size_t hashVal = 0;
        for (auto &f : groupingFuncs) {
            hashVal ^= f()->hash();
        }

        // update the attribute in the aggregate record for each aggregate we are computing
        for (size_t i = 0; i < aggComps.size(); i++) {
            aggRecord->getAtt(i)->set(aggComps[i]());
        }

        void* location = aggPages.back().appendAndReturnLocation(aggRecord);
        myHash[hashVal] = location;

        // 5, 10, 15, 


    }

    // for the final step of aggregate function, iterate over the hash map and append everything into the output table
    // NOTES: there are not new test cases from the current testing suite
}

#endif

