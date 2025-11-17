
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
        MyDB_PageReaderWriter page = input->operator[](i);

        if (page.getType () == MyDB_PageType :: RegularPage)
        {
            allPages.push_back (page);
        }
    } 

    // create input record and iterator
    MyDB_RecordPtr inputRec = input->getEmptyRecord ();
    auto iter = getIteratorAlt (allPages);

    // represents the pinned anaonymous pages we add our aggregrate records to
    vector <MyDB_PageReaderWriter> aggPages;
    aggPages.push_back(MyDB_PageReaderWriter(true, *input->getBufferMgr()));

    // create a schema that can store all of the required aggregate and grouping attributes
    MyDB_SchemaPtr aggSchema = make_shared <MyDB_Schema> ();
    for (auto &p : output->getTable ()->getSchema ()->getAtts ())
    {
        aggSchema->appendAtt (p);
    }

    // add an extra COUNT
    string countName = "COUNT";
    pair<string, MyDB_AttTypePtr> countAtt = {countName, make_shared <MyDB_IntAttType> () };
    aggSchema->appendAtt(countAtt);
    MyDB_RecordPtr aggRec = make_shared <MyDB_Record> (aggSchema);

    vector<pair<string, MyDB_AttTypePtr>> aggAttributes = aggSchema->getAtts();
    // create the combined record schema
    MyDB_SchemaPtr combinedSchema = make_shared <MyDB_Schema> ();
    for (auto &p : aggAttributes) {
        combinedSchema->appendAtt(p);
    }

    for (auto &p : inputRec->getSchema()->getAtts()) {
        combinedSchema->appendAtt(p);
    }

    // create the combined record
    MyDB_RecordPtr combinedRec = make_shared <MyDB_Record> (combinedSchema);
    combinedRec->buildFrom (aggRec, inputRec);

    // have a function for each grouping clause
    vector <func> groupingFuncs;
    for (size_t i = 0; i < groupings.size(); i++) {
        groupingFuncs.push_back(inputRec->compileComputation(groupings[i]));
    }

    vector<pair<string, MyDB_AttTypePtr>> combinedRecAttributes = combinedSchema->getAtts();
    // NOTE: grouping attributes are always first in the output/aggregrate record schema
    int numGroups = groupings.size();
    cout << "about to make the agg functions \n" << flush;
    // have a function for each aggregate
    vector <func> aggComps;
    vector <func> defaultAggComps;
    for (size_t i = 0; i < aggsToCompute.size(); i++) {
        string aggString;
        string defaultAggString;
        // get the aggregate 
        pair<MyDB_AggType, string> agg = aggsToCompute[i];
        int oldAgIndex = i + numGroups;
        string oldAggString = aggAttributes[oldAgIndex].first;

        // means we take the sum of the old agg + the new input record
        if (agg.first == MyDB_AggType :: sum || agg.first == MyDB_AggType :: avg) {
            aggString = "+ (" + agg.second + ", [" + oldAggString + "])";
            defaultAggString = agg.second;
        } else if (agg.first == MyDB_AggType :: cnt) {
            aggString = "+ ([" + oldAggString + "], int[1])";
            defaultAggString = "int[1]";
        }
        cout << "About to compile the agg functions " << aggString << "\n" << flush;
        aggComps.push_back(combinedRec->compileComputation(aggString));
        defaultAggComps.push_back(combinedRec->compileComputation(defaultAggString));
    }

    cout << "finished making the agg functions \n" << flush;
    // add the count aggregate
    aggComps.push_back(combinedRec->compileComputation("+ ([" + countName + "], int[1])"));
    defaultAggComps.push_back(combinedRec->compileComputation("int[0]"));

    cout << "finished making the count agg function \n" << flush;

    func pred = inputRec->compileComputation (selectionPredicate);

    // go through the input table
    while (iter->advance()) {
        iter->getCurrent(inputRec);
        // skip the ones not accepted by the predicate
        if (!pred()->toBool()) {
            continue;
        }
        
        size_t hashVal = 0;
        // go through each grouping func
        for (size_t i = 0; i < groupings.size(); i++) {
            auto &f = groupingFuncs[i];
            // set the grouping attribute value
            aggRec->getAtt(i)->set(f());
            // hash the current record 
            hashVal ^= f()->hash();
        }

        // update the attribute in the aggregate record for each aggregate we are computing

        // check if this aggregation exists in our hash table
        auto it = myHash.find(hashVal);

        // means it's a new aggregation
        if (it == myHash.end()) { 
            for (size_t i = 0; i < aggComps.size(); i++) {
                aggRec->getAtt(i+numGroups)->set(defaultAggComps[i]()); // Smth not right here
            }
            // aggRec->getAtt(numGroups + aggsToCompute.size())->set(aggComps.back()());   // internal count

            aggRec->recordContentHasChanged();
            void* location = aggPages.back().appendAndReturnLocation(aggRec);
            
            // check there is enough room in this last page
            if (location == nullptr) {
                // add another pinned anonymous page to this vector
                aggPages.push_back(MyDB_PageReaderWriter(false, *input->getBufferMgr()));
                location = aggPages.back().appendAndReturnLocation(aggRec);
            }
            
            // new aggregation value
            myHash[hashVal] = location;
        } else {
            // get the current aggregate
            void* curLocation = myHash[hashVal];
            aggRec->fromBinary(curLocation);

            // update the current aggregate
            for (size_t i = 0; i < aggComps.size(); i++) {
                aggRec->getAtt(i + numGroups)->set(aggComps[i]());
            }
            aggRec->recordContentHasChanged();
            // write it back after it's been updated
            aggRec->toBinary(curLocation);
        }
    }

    cout << "about to make the final agg functions \n" << flush;
    vector <func> finalAggComps;
    // create the final aggregation funcs
    for (size_t i = 0; i < aggsToCompute.size(); i++) { 
        string aggString;
        string oldAggString = aggAttributes[i + numGroups].first;
        pair<MyDB_AggType, string> agg = aggsToCompute[i];
        // if the aggregation is avg then we divide by the count
        if (aggsToCompute[i].first == MyDB_AggType :: avg) { 
            aggString = "/ ([" + oldAggString + "], [" + countName + "])";
        } else {
            // otherwise just return the same value
            aggString = "[" + oldAggString + "]";
        }

        cout << "About to compile the final agg functions " << aggString << "\n" << flush;

        finalAggComps.push_back(aggRec->compileComputation(aggString));
    }

    cout << "finished making the final agg functions \n" << flush;

    // for the final step of aggregate function, iterate over the hash map and append everything into the output table
    MyDB_RecordPtr outRec = output->getEmptyRecord ();
    // iterate over the hashmap
    for (const auto& pair : myHash) {        
        aggRec->fromBinary(pair.second);
        
        // set the grouping atts
        int i;
        for (i = 0; i < numGroups; i++) {
                outRec->getAtt (i)->set (aggRec->getAtt (i));
        }

        // set the aggregate atts
        for (auto a : finalAggComps) {
            outRec->getAtt (i++)->set (a ());
        }

        outRec->recordContentHasChanged ();
        output->append (outRec);
    } 


    // NOTES: there are not new test cases from the current testing suite
}

#endif