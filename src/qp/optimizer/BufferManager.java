/**
 * simple buffer manager that distributes the buffers equally among all the join operators
 **/

package qp.optimizer;

public class BufferManager {

    static int numBuffer;
    static int numJoinAndSort;

    static int buffPerJoinAndSort;

    public BufferManager(int numBuffer, int numJoinAndSort) {
        this.numBuffer = numBuffer;
        this.numJoinAndSort = numJoinAndSort;
        buffPerJoinAndSort = numBuffer / numJoinAndSort;
    }

    public static int getBuffersPerJoinAndSort() {
        return buffPerJoinAndSort;
    }

}
