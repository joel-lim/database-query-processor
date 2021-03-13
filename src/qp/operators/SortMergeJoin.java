/**
 * Sort Merge Join algorithm
 **/

package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class SortMergeJoin extends Join {

    int batchsize; // Number of tuples per out batch
    ArrayList<Integer> leftindex; // Indices of the join attributes in left table
    ArrayList<Integer> rightindex; // Indices of the join attributes in right table
    Batch outbatch; // Buffer page for output
    Batch leftbatch; // Buffer page for left input stream
    Batch rightbatch; // Buffer page for right input stream

    int lcurs; // Cursor for left side buffer
    int rcurs; // Cursor for right side buffer
    boolean eosl; // Whether end of stream (left table) is reached
    boolean eosr; // Whether end of stream (right table) is reached

    Tuple leftTuple; // Current left tuple
    Tuple rightTuple; // Current right tuple
    ArrayList<Batch> partition = new ArrayList<>(); // Current partition
    boolean isNewPartition = true;
    int partitionBatchNo = 0; // Pointer to current partition batch
    int partitionTupleNo = 0; // Pointer to current tuple in current partition batch
    ObjectInputStream partitionIn; // Partition file (if buffer size too large)

    public SortMergeJoin(Join jn) {
        super(jn.getLeft(), jn.getRight(), jn.getConditionList(), jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }

    /**
     * During open finds the index of the join attributes * Materializes the right
     * hand side into a file * Opens the connections
     **/
    public boolean open() {
        /** select number of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize = Batch.getPageSize() / tuplesize;

        /** find indices attributes of join conditions **/
        leftindex = new ArrayList<>();
        rightindex = new ArrayList<>();
        for (Condition con : conditionList) {
            Attribute leftattr = con.getLhs();
            Attribute rightattr = (Attribute) con.getRhs();
            leftindex.add(left.getSchema().indexOf(leftattr));
            rightindex.add(right.getSchema().indexOf(rightattr));
        }

        /** initialize the cursors of input buffers **/
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        eosr = false;

        return left.open() && right.open();
    }

    /**
     * from input buffers selects the tuples satisfying join condition * And returns
     * a page of output tuples
     **/
    public Batch next() {
        if (eosl || eosr) {
            return null;
        }

        if (leftTuple == null) {
            // handles first call
            leftbatch = left.next();
            rightbatch = right.next();
            advanceLeft();
            advanceRight();
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull() && !eosl && !eosr) {
            int compare = Tuple.compareTuples(leftTuple, rightTuple, leftindex, rightindex);

            if (compare < 0) {
                // left is smaller than right
                advanceLeft();
                // backtrack on the current partition if it exists
                if (!partition.isEmpty()) {
                    backtrackPartition();
                }
            } else if (compare > 0) {
                // right is smaller than left (we are at the end of the partition at this point)
                advanceRight();
                // clear partition as it will no longer match with any left tuples
                clearPartition();
            } else {
                // tuples satisfy the join condition
                Tuple outTuple = leftTuple.joinWith(rightTuple);
                outbatch.add(outTuple);

                // if partition is new, we must start building up a new partition for future left tuples to refer to
                if (isNewPartition) {
                    addToPartition();
                    advanceRight();
                } else {
                    // if we reached the end of the partition, backtrack to the start and advance left ptr
                    if (isEndOfPartition()) {
                        backtrackPartition();
                        advanceLeft();
                    } else {
                        // if we are halfway through the partition, continue iterating through it
                        advancePartition();
                    }
                }
            }
            if (outbatch.isFull()) {
                return outbatch;
            }
        }
        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        // File f = new File(rfname);
        // f.delete();
        return true;
    }

    private void advanceLeft() {
        // Check if left is exhausted
        if (leftbatch == null) {
            eosl = true;
            leftTuple = null;
            return;
        }

        // Get new batch if reached end of current batch
        if (lcurs == leftbatch.size()) {
            lcurs = 0;
            leftbatch = left.next();
        }

        // If no more batches, mark eosl and return null tuple
        if (leftbatch == null || leftbatch.isEmpty()) {
            eosl = true;
            leftTuple = null;
            return;
        }

        // Read the next tuple, update cursor
        leftTuple = leftbatch.get(lcurs);
        lcurs += 1;
    }

    private void advanceRight() {
        // Check if right is exhausted
        if (rightbatch == null) {
            eosr = true;
            rightTuple = null;
            return;
        }

        // Get new batch if reached end of current batch
        if (rcurs == rightbatch.size()) {
            rcurs = 0;
            rightbatch = right.next();
        }

        // If no more batches, mark eosr and return
        if (rightbatch == null || rightbatch.isEmpty()) {
            eosr = true;
            rightTuple = null;
            return;
        }

        // Read the next tuple, update cursor
        rightTuple = rightbatch.get(rcurs);
        rcurs += 1;
    }

    private void addToPartition() {
        // ignores max num of buffers for now
        // TODO: write partition to file if num buffers exceeded?
        Batch currentBatch;

        // handles first buffer
        if (partition.isEmpty()) {
            currentBatch = new Batch(batchsize);
            partition.add(currentBatch);
        } else {
            currentBatch = partition.get(partitionBatchNo);
        }

        // add tuple to batch, creating new batch if necessary
        if (currentBatch.isFull()) {
            Batch newBatch = new Batch(batchsize);
            newBatch.add(rightTuple);
            partition.add(newBatch);
        } else {
            currentBatch.add(rightTuple);
        }
    }

    private void advancePartition() {
        if (partitionTupleNo == batchsize - 1) {
            // get a new batch if we have reached the end of the current one
            partitionBatchNo += 1;
            partitionTupleNo = 0;
        } else {
            // else just advance the ptr in the current batch
            partitionTupleNo += 1;
        }
        rightTuple = partition.get(partitionBatchNo)
                              .get(partitionTupleNo);
    }

    private void backtrackPartition() {
        isNewPartition = false;
        partitionBatchNo = 0;
        partitionTupleNo = 0;
    }

    private void clearPartition() {
        isNewPartition = true;
        partition = new ArrayList<>();
        partitionBatchNo = 0;
        partitionTupleNo = 0;
    }

    private boolean isEndOfPartition() {
        return (partitionBatchNo == partition.size() - 1 &&
                partitionTupleNo == partition.get(partitionBatchNo).size() - 1);
    }
}
