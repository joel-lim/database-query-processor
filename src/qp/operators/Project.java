package qp.operators;

import java.io.File;
import java.util.ArrayList;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Schema;
import qp.utils.Tuple;
import qp.utils.TupleReader;
import qp.utils.TupleWriter;

public class Project extends Operator {
    Operator base;
    int numBuff;
    boolean distinct;
    int batchSize;
    ArrayList<Attribute> attributeList;
    int[] projectedIxes;

    // Distinct projection
    ArrayList<File> sortedRuns;
    ArrayList<TupleReader> inBuffers; // for multi-way merging
    int fileId; // unique id for files generated

    // Regular projection
    Batch inbatch; // for simple projection (no distinct)
    int incur; // pointer to next pointer in inbatch
    boolean eos; // only used in non-distinct projection to indicate base operator has finished.

    public Project(Operator base, ArrayList<Attribute> attributes, boolean distinct, int optype, int numBuff) {
        super(optype);
        this.setSchema(base.getSchema());

        this.base = base;
        this.attributeList = attributes;
        this.distinct = distinct;
        this.numBuff = numBuff;
        this.fileId = 0;
    }

    public Operator getBase() {
        return base;
    }

    public void setBase(Operator base) {
        this.base = base;
    }
    
    public ArrayList<Attribute> getProjAttr() {
        return this.attributeList;
    }

    public boolean isDistinct() {
        return this.distinct;
    }

    // If distinct, open consumes base operator input. Non-distinct project streams input like regular (upon call to next)
    @Override
    public boolean open() {
        int tuplesize = schema.getTupleSize(); // this is the projected schema (subschema)
        this.batchSize = Batch.getPageSize() / tuplesize;
        // precompute indices for projection
        this.projectedIxes = new int[this.attributeList.size()];
        for (int i = 0; i < this.projectedIxes.length; i++) {
            Attribute attr = this.attributeList.get(i);
            if (attr.getAggType() != Attribute.NONE) {
                System.err.println("Aggregation is not implemented.");
                System.exit(1);
            }
            // At this point attr is a base attribute, no need to call Attribute.getBaseAttribute();
            this.projectedIxes[i] = this.base.getSchema().indexOf(attr);
        }
        if (this.base.open()) {
            if (this.distinct) {
                this.sortedRuns = new ArrayList<>();
                this.inBuffers = new ArrayList<>(this.numBuff - 1);
                this.generateProjectedSortedRuns();
                this.mergeAndDedupRuns();
            }            
            return true;
        } else {
            return false;
        }
    }

    // Handles distinct next (involving merging sortedRuns) in separate method nextDistinct
    @Override
    public Batch next() {
        if (this.distinct) {
            return this.nextDistinct();
        } else {
            if (this.eos) {
                return null;
            }
            Batch outbatch = new Batch(this.batchSize);
            while (!outbatch.isFull()) {
                // no current input batch, try to read next
                if (this.inbatch == null) {
                    this.inbatch = base.next();
                    if (this.inbatch == null) {
                        // no more input, close stream and set eos to return null on future nexts
                        this.eos = true;
                        base.close();
                        return outbatch.isEmpty() ? null : outbatch;
                    }
                }
                // inbatch is not null
                if (this.incur < this.inbatch.size()) {
                    outbatch.add(this.project(this.inbatch.get(this.incur)));
                    this.incur++;
                } else {
                    this.incur = 0;
                    this.inbatch = null;
                }
            }

            return outbatch;
        }
    }

    private Batch nextDistinct() {
        if (this.inBuffers.isEmpty()) {
            return null;
        }
        Batch outbatch = new Batch(this.batchSize);
        Tuple prev = null;
        while (!outbatch.isFull() && !this.inBuffers.isEmpty()) {
            
            int indexMin = 0;
            Tuple minTuple = null;
            int indexCurr = 0;
            while (indexCurr < this.inBuffers.size()) {
                Tuple tup = this.inBuffers.get(indexCurr).peek();
                // consume duplicates of prev, if prev exists and this buffer is not empty.
                while (prev != null && tup != null && prev.equals(tup)) {
                    this.inBuffers.get(indexCurr).next();
                    tup = this.inBuffers.get(indexCurr).peek();
                }
                if (tup == null) {
                    this.inBuffers.remove(indexCurr).close();
                    this.sortedRuns.remove(indexCurr).delete();
                    // do not increment indexCurr
                    continue;
                } else if (minTuple == null || tup.compareTo(minTuple) < 0) {
                    minTuple = tup;
                    indexMin = indexCurr;
                } 
                indexCurr++;
            }
            
            // inBuffers may have become empty within the loop
            if (this.inBuffers.isEmpty()) {
                break;
            }
            prev = this.inBuffers.get(indexMin).next();
            outbatch.add(prev);
        }
        return outbatch;
    }

    @Override
    public boolean close() {
        if (this.inBuffers != null) {
            this.inBuffers.clear();
        }
        return true;
    }

    private void generateProjectedSortedRuns() {
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
        this.base.close();
    }

    private void sortAndWrite(ArrayList<Batch> buffers) {
        File sortedRun = new File(this.getUniqueFileName());
        this.sortedRuns.add(sortedRun);
        TupleWriter out = new TupleWriter(sortedRun.getName(), this.batchSize);
        if (!out.open()) {
            System.err.println("Sort: Error in writing file");
            System.exit(1);
        }
        buffers.stream()
            .flatMap(buff -> buff.stream())
            .map(this::project)
            .sorted()
            .distinct()
            .forEachOrdered(tup -> out.next(tup)); 
        out.close();
    }

    private Tuple project(Tuple inputTuple) {
        ArrayList<Object> projected = new ArrayList<>(this.projectedIxes.length);
        for (int i : this.projectedIxes) {
            projected.add(inputTuple.dataAt(i));
        }
        return new Tuple(projected);
    }


    private void mergeAndDedupRuns() {
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
                    File nextSortedRun = this.mergeAndDedup();
                    nextSortedRuns.add(nextSortedRun);
                }
            }
            // merge any leftover sorted runs
            if (!this.inBuffers.isEmpty()) {
                File nextSortedRun = this.mergeAndDedup();
                nextSortedRuns.add(nextSortedRun);
            }
        
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

    private File mergeAndDedup() {
        File nextSortedRun = new File(this.getUniqueFileName());
        TupleWriter outBuffer = new TupleWriter(nextSortedRun.getName(), this.batchSize);
        if (!outBuffer.open()) {
            System.err.println("Project: Error in opening file for writing");
            System.exit(1);
        }
        Tuple prev = null;
        while (this.inBuffers.size() > 0) {
            int indexMin = 0;
            Tuple minTuple = null;
            int indexCurr = 0;
            while (indexCurr < this.inBuffers.size()) {
                Tuple tup = this.inBuffers.get(indexCurr).peek();
                // consume duplicates
                while (prev != null && tup != null && prev.equals(tup)) {
                    this.inBuffers.get(indexCurr).next();
                    tup = this.inBuffers.get(indexCurr).peek();
                }
                if (tup == null) {
                    this.inBuffers.remove(indexCurr).close();
                    // do not increment indexCurr
                    continue;
                } else if (minTuple == null || tup.compareTo(minTuple) < 0) {
                    minTuple = tup;
                    indexMin = indexCurr;
                } 
                indexCurr++;
            }            
            // inBuffers may have become empty within the loop
            if (this.inBuffers.isEmpty()) {
                break;
            }
            
            prev = this.inBuffers.get(indexMin).next();
            outBuffer.next(prev);
        }
        outBuffer.close();
        this.inBuffers.clear();
        return nextSortedRun;
    }

    private String getUniqueFileName() {
        return "DISTINCT-" + (this.fileId++);
    }

    public Object clone() {
        Operator newbase = (Operator) base.clone();
        ArrayList<Attribute> newattr = new ArrayList<>();
        for (int i = 0; i < attributeList.size(); ++i)
            newattr.add((Attribute) attributeList.get(i).clone());
        Project newproj = new Project(newbase, newattr, this.distinct, optype, this.numBuff);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }
}
