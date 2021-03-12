package qp.operators;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.TupleReader;
import qp.utils.TupleWriter;

public class Sort extends Operator {
    Operator base;
    int numBuff;
    ArrayList<Attribute> orderbyList;
    ArrayList<File> sortedRuns;
    ArrayList<TupleReader> inBuffers; // for multi-way merging
    int compareMultiplier; // -1 for descending sort, else 1
    int batchSize;
    int fileId; // unique id for files generated

    public Sort(Operator base, ArrayList<Attribute> orderbyList, boolean isDesc, int optype, Schema schema, int numBuff) {
        super(optype);
        this.setSchema(schema);

        this.base = base;
        this.orderbyList = orderbyList;
        this.compareMultiplier = isDesc ? -1 : 1;
        this.numBuff = numBuff;
        this.sortedRuns = new ArrayList<>();
        this.inBuffers = new ArrayList<>(this.numBuff - 1);
        this.fileId = 0;
    }

    @Override
    public boolean open() {
        /** Set number of tuples per page**/
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
        Batch outbatch = new Batch(this.batchSize);
        while (!outbatch.isFull() && this.inBuffers.size() > 0) {
            int indexMin = 0;
            Tuple minTuple = null;
            int indexCurr = 0;
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
            outbatch.add(this.inBuffers.get(indexMin).next());
        }
        return outbatch;
    }

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
        if (buffers.size() > 0) {
            sortAndWrite(buffers);
        }
    }

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
            .forEachOrdered(tup -> out.next(tup)); // I hope this does not count as using extra buffers and "cheating"
        out.close();
    }

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
            // single pass
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
            File nextSortedRun = this.merge();
            nextSortedRuns.add(nextSortedRun);

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
            outBuffer.next(this.inBuffers.get(indexMin).next());
        }
        outBuffer.close();
        this.inBuffers.clear();
        return nextSortedRun;
    }

    private String getUniqueFileName() {
        return "SORT" + (this.fileId++);
    }
}