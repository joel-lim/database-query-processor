package qp.operators;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Tuple;
import qp.utils.TupleReader;
import qp.utils.TupleWriter;

public class Sort extends Operator {
    static int currentSortId = 0; // unique id for files between multiple sorts

    Operator base;
    int numBuff;
    ArrayList<Attribute> orderbyList;
    ArrayList<File> sortedRuns;
    ArrayList<TupleReader> inBuffers; // for multi-way merging
    int compareMultiplier; // -1 for descending sort, else 1
    int batchSize;
    int fileId; // unique id for files generated
    int sortId;

    public Sort(Operator base, ArrayList<Attribute> orderbyList, boolean isDesc, int optype, int numBuff) {
        super(optype);
        this.setSchema(base.getSchema());

        this.base = base;
        this.orderbyList = orderbyList;
        this.compareMultiplier = isDesc ? -1 : 1;
        this.numBuff = numBuff;
        this.sortedRuns = new ArrayList<>();
        this.inBuffers = new ArrayList<>(this.numBuff - 1);
        this.fileId = 0;
        this.sortId = Sort.currentSortId++;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }

    // Prepare pipeline for producing sorted output. Take note that since sorting is 
    // not a very streamable operation, given that it has a preparation phase (generating and merging
    // sorted runs), open already consumes output of base Operator (hence effectively consuming the
    // entire pipeline) and stops before the final pass of merging (which would produce the final output)
    // which takes place in next()
    @Override
    public boolean open() {
        this.setSchema(base.getSchema());
        int tuplesize = schema.getTupleSize();
        this.batchSize = Batch.getPageSize() / tuplesize;
        if (this.base.open()) {
            this.generateSortedRuns();
            this.mergeRuns();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public Batch next() {
        if (this.inBuffers.isEmpty()) {
            return null;
        }
        Batch outbatch = new Batch(this.batchSize);
        while (!outbatch.isFull() && !this.inBuffers.isEmpty()) {
            
            int indexMin = 0;
            Tuple minTuple = null;
            int indexCurr = 0;
            // iterate through buffers and find the smallest value in order given by getOrder()
            while (indexCurr < this.inBuffers.size()) {
                Tuple tup = this.inBuffers.get(indexCurr).peek();
                if (tup == null) {
                    // remove empty input buffers and delete them from secondary storage
                    // once we have read them fully
                    this.inBuffers.remove(indexCurr).close();
                    this.sortedRuns.remove(indexCurr).delete();
                    // do not increment indexCurr
                    continue;
                } else if (minTuple == null || getOrder().compare(tup, minTuple) < 0) {
                    minTuple = tup;
                    indexMin = indexCurr;
                } 
                indexCurr++;
            }
            
            // inBuffers may have become empty within the loop
            if (this.inBuffers.isEmpty()) {
                break;
            }
            outbatch.add(this.inBuffers.get(indexMin).next());
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        this.inBuffers.clear(); // make sure subsequent calls to next return null
        return true;
    }

    // Read in numBuff buffers from base, then sort and write them out into different files
    private void generateSortedRuns() {
        Batch inbatch;
        ArrayList<Batch> buffers = new ArrayList<>(this.numBuff);
        while ((inbatch = this.base.next()) != null) {
            buffers.add(inbatch);
            if (buffers.size() == this.numBuff) {
                sortAndWrite(buffers);
                buffers.clear();
            }
        }
        // sort and write out any remaining buffers 
        if (buffers.size() > 0) {
            sortAndWrite(buffers);
        }
        this.base.close();
    }

    // Perform in-memory sort of our ArrayList of buffers, then write it out into a new file (stored in sortedRuns)
    private void sortAndWrite(ArrayList<Batch> buffers) {
        File sortedRun = new File(this.getUniqueFileName());
        sortedRuns.add(sortedRun);
        TupleWriter out = new TupleWriter(sortedRun.getName(), this.batchSize);
        if (!out.open()) {
            System.err.println("Sort: Error in writing file");
            System.exit(1);
        }
        buffers.stream()
            .flatMap(buff -> buff.stream())
            .sorted(getOrder())
            .forEachOrdered(tup -> out.next(tup)); // stream output into file single batch at a time
        out.close();
    }

    // Sorting order of tuples given by this comparator. Compares each attribute in the orderbylist
    // in declared order, only checking the next attribute if the 2 tuples are equal on this attribute
    // Reverses order if descending (compareMultiplier is -1 for descending)
    private Comparator<Tuple> getOrder() {
        return (t1, t2) -> {
            for (Attribute attr : orderbyList) {
                int compIx = this.getSchema().indexOf(attr);
                int res = this.compareMultiplier * Tuple.compareTuples(t1, t2, compIx);
                if (res != 0) {
                    return res;
                }
            }
            return 0;
        };
    }

    private void mergeRuns() {
        // iterate until final pass
        while (sortedRuns.size() > this.numBuff - 1) {
            // single pass. Read in each sorted run into 1 input buffer, then merge them when inBuffers is full
            // and write them out again.
            ArrayList<File> nextSortedRuns = new ArrayList<>();
            for (File sortedRun : this.sortedRuns) {
                TupleReader in = new TupleReader(sortedRun.getName(), this.batchSize);
                if (!in.open()) {
                    System.err.println("Sort: Error in opening sorted run for reading");
                    System.exit(1);
                }
                this.inBuffers.add(in);
                if (this.inBuffers.size() == this.numBuff - 1) {
                    File nextSortedRun = this.merge();
                    nextSortedRuns.add(nextSortedRun);
                }
            }
            // merge any leftover sorted runs
            if (!this.inBuffers.isEmpty()) {
                File nextSortedRun = this.merge();
                nextSortedRuns.add(nextSortedRun);
            }
        
            // Previous set of runs no longer needed
            for (File sortedRun : this.sortedRuns) {
                sortedRun.delete();
            }
            this.sortedRuns = nextSortedRuns;
        }

        // Populate buffers ready to produce sorted output upon call to next()
        for (File sortedRun : this.sortedRuns) {
            TupleReader in = new TupleReader(sortedRun.getName(), this.batchSize);
            if (!in.open()) {
                System.err.println("Sort: Error in opening sorted run for reading");
                System.exit(1);
            }
            this.inBuffers.add(in);
        }
    }

    // Merges current input buffers, outputing them into a single sorted run on disk.
    // Returns a File object representing this sorted run stored on disk
    private File merge() {
        File nextSortedRun = new File(this.getUniqueFileName());
        TupleWriter outBuffer = new TupleWriter(nextSortedRun.getName(), this.batchSize);
        if (!outBuffer.open()) {
            System.err.println("Sort: Error in opening file for writing");
            System.exit(1);
        }
        while (this.inBuffers.size() > 0) {
            int indexMin = 0;
            Tuple minTuple = null;
            int indexCurr = 0;
            // iterate through buffers and find the smallest value in order given by getOrder()
            while (indexCurr < this.inBuffers.size()) {
                Tuple tup = this.inBuffers.get(indexCurr).peek();
                if (tup == null) {
                    this.inBuffers.remove(indexCurr).close();
                    // do not increment indexCurr
                    continue;
                } else if (minTuple == null || getOrder().compare(tup, minTuple) < 0) {
                    minTuple = tup;
                    indexMin = indexCurr;
                } 
                indexCurr++;
            }            
            // inBuffers may have become empty within the loop
            if (this.inBuffers.isEmpty()) {
                break;
            }
            outBuffer.next(this.inBuffers.get(indexMin).next());
        }
        outBuffer.close();
        this.inBuffers.clear();
        return nextSortedRun;
    }

    private String getUniqueFileName() {
        return "SORT" + (this.sortId) + "-" + (this.fileId++);
    }
}
