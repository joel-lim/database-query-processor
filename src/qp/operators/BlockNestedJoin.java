package qp.operators;

import qp.utils.Attribute;
import qp.utils.Batch;
import qp.utils.Condition;
import qp.utils.Tuple;

import java.io.*;
import java.util.ArrayList;

public class BlockNestedJoin extends Join {

    static int filenum = 0; // To get unique filenum for this operation
    int batchsize; // Number of tuples per out batch
    ArrayList<Integer> leftindex; // Indices of the join attributes in left table
    ArrayList<Integer> rightindex; // Indices of the join attributes in right table
    String rfname; // The file name where the right table is materialized
    Batch outbatch; // Buffer page for output
    ArrayList<Batch> leftbatch; // Buffer page for left input stream
    Batch rightbatch; // Buffer page for right input stream
    ObjectInputStream in; // File pointer to the right hand materialized file

    int lcurs; // Cursor for left side current buffer
    int lbcurs; // Cursor for lesft side buffer
    int rcurs; // Cursor for right side buffer
    boolean eosl; // Whether end of stream (left table) is reached
    boolean eosr; // Whether end of stream (right table) is reached

    public BlockNestedJoin(Join jn) {
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
        Batch rightpage;

        /** initialize the cursors of input buffers **/
        lbcurs = 0;
        lcurs = 0;
        rcurs = 0;
        eosl = false;
        /**
         * because right stream is to be repetitively scanned if it reached end, we have
         * to start new scan
         **/
        eosr = true;

        /**
         * Right hand side table is to be materialized for the Nested join to perform
         **/
        if (!right.open()) {
            return false;
        } else {
            /**
             * If the right operator is not a base table then Materialize the intermediate
             * result from right into a file
             **/
            filenum++;
            rfname = "BNJtemp-" + String.valueOf(filenum);
            try {
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while ((rightpage = right.next()) != null) {
                    out.writeObject(rightpage);
                }
                out.close();
            } catch (IOException io) {
                System.out.println("BlockNestedJoin: Error writing to temporary file");
                return false;
            }
            if (!right.close())
                return false;
        }
        if (left.open())
            return true;
        else
            return false;
    }

    public Batch next() {
        if (eosl) {
            return null;
        }
        outbatch = new Batch(batchsize);
        while (!outbatch.isFull()) {
            if (lcurs == 0 && eosr == true) {
                /** new left pages is to be fetched **/
                leftbatch = new ArrayList<>(numBuff - 2);
                for (int i = 0; i < numBuff - 2; i++) {
                    Batch inbatch;
                    if ((inbatch = left.next()) != null) {
                        leftbatch.add(inbatch);
                    } else {
                        eosl = true;
                        return outbatch;
                    }
                }
                /**
                 * Whenever a new left page comes, we have to start the scanning of right table
                 **/
                try {
                    in = new ObjectInputStream(new FileInputStream(rfname));
                    eosr = false;
                } catch (IOException io) {
                    System.err.println("BlockNestedJoin:error in reading the file");
                    System.exit(1);
                }
            }

            while (eosr == false) {
                try {
                    if (rcurs == 0 && lcurs == 0) {
                        rightbatch = (Batch) in.readObject();
                    }
                    for (int i = lbcurs; i < leftbatch.size(); i++) {
                        for (int j = lcurs; j < leftbatch.get(i).size(); j++) {
                            for (int k = rcurs; k < rightbatch.size(); k++) {
                                Tuple lefttuple = leftbatch.get(i).get(j);
                                Tuple righttuple = rightbatch.get(k);
                                if (lefttuple.checkJoin(righttuple, leftindex, rightindex)) {
                                    Tuple outtuple = lefttuple.joinWith(righttuple);
                                    outbatch.add(outtuple);
                                    if (outbatch.isFull()) {
                                        if (i == leftbatch.size() - 1 && j == leftbatch.get(i).size() - 1
                                                && k == rightbatch.size() - 1) {
                                            lcurs = 0;
                                            lbcurs = 0;
                                            rcurs = 0;
                                        } else if (i != leftbatch.size() - 1 && j == leftbatch.get(i).size() - 1
                                                && k == rightbatch.size() - 1) {
                                            lbcurs = i + 1;
                                            lcurs = 0;
                                            rcurs = 0;
                                        } else if (j != leftbatch.get(i).size() - 1 && k == rightbatch.size() - 1) {
                                            lbcurs = i;
                                            lcurs = j + 1;
                                            rcurs = 0;
                                        } else {
                                            lbcurs = i;
                                            lcurs = j;
                                            rcurs = k + 1;
                                        }
                                        return outbatch;
                                    }
                                }
                            }
                            rcurs = 0;
                        }
                        lcurs = 0;
                    }
                    lbcurs = 0;
                } catch (EOFException e) {
                    try {
                        in.close();
                    } catch (IOException io) {
                        System.out.println("BlockNestedJoin: Error in reading temporary file");
                    }
                    eosr = true;
                } catch (ClassNotFoundException c) {
                    System.out.println("BlockNestedJoin: Error in deserialising temporary file ");
                    System.exit(1);
                } catch (IOException io) {
                    System.out.println("BlockNestedJoin: Error in reading temporary file");
                    System.exit(1);
                }
            }
        }

        return outbatch;
    }

    /**
     * Close the operator
     */
    public boolean close() {
        File f = new File(rfname);
        f.delete();
        return true;
    }

}
