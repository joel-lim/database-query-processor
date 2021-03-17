# On Query Processing

## 1: Implementation of Block Nested Loops Join
The implementation of Block Nested Loops Join is mainly found [here](src/qp/operators/BlockNestedJoin.java).

Block Nested Loops Join algorithm is as follows:

1. Read the outer relation of block size (B - 2) where B is the number of buffers.
2. Read one batch of the inner relation into a buffer.
3. For all the tuples in the block of the outer relation, compare it with all the tuples of the inner relation in the buffer.
4. Continue reading the inner relation batch by batch until it reaches the end (in other words, scan all tuples of left block with all the tuples in the inner relation). Pairs of tuples that satisfy the condition are added into the output buffer.
5. Consider the next block of outer relation and repeat.


## 2: Implementation of Sort Merge Join
The implementation of Sort Merge Join is mainly found [here](src/qp/operators/SortMergeJoin.java).

Sort Merge Join is implemented by first using a [Sort operator](src/qp/operators/Sort.java) based on the join attributes on both the left and right relations, then merging them.

The merge algorithm used is as follows:

1. Advance scan of R until current R-tuple's sort key >= current S-tuple's sort key
2. Advance scan of S until current S-tuple's sort key >= current R-tuple's sort key 
3. While R-tuple's sort key == S-tuple's sort key, output(R,S), add the S-tuple to an S-partition and advance the scan of S.
4. Advance scan of R until R-tuple's sort key > S-partition's sort key, while outputting all pairs of (R,S) for each S in the S-partition.
5. Clear the S-partition, repeat

Assumptions:

1. The S-partition is able to fit in memory

## 3: Implementation of ORDERBY

The ORDERBY clause is implemented via a new [Sort](src/qp/operators/Sort.java) Operator, which implements the external sort algorithm. The Sort operator is the final evaluation step and thus is the root Operator of the query plan. (Refer to [RandomInitialPlan.prepareInitialPlan](src/qp/optimizer/RandomInitialPlan.java) - `createSortOp` is called last, after `createProjectOp`).

Each Sort Operator takes a list of Attributes from the input table to sort on, as well as a boolean flag isDesc to indicate if the sort should be in descending order. The isDesc flag sets `compareMultiplier` which is -1 if the sort is descending, thus flipping the order of comparison given by `Tuple.compareTuples`.

Each Sort performed also makes use of the Buffer pool. The BufferManager is thus modified to share the buffers amongst the total number of Joins and Sorts performed equally.

The external sort algorithm requires a preparation phase where it generates and merges sorted runs, before it can finally produce sorted output, when the number of sorted runs on disk <= number of buffers - 1. Thus the call to `Sort.open` has to consume output from the base operator. This effectively breaks up the pipeline into separate stages.

The partially sorted runs are stored on disk as files given a unique name per sort and per run within sort (see `getUniqueFileName` at bottom of [Sort.java](src/qp/operators/Sort.java)). The temporary files are tracked in an instance variable `ArrayList<File> sortedRuns`. At each merging pass, after the run has been merged with other runs, the backing file is removed.

Lastly, its IO cost is estimated in [PlanCost.java](src/qp/optimizer/PlanCost.java) as per the formula given in lecture = 2 * ceil(log<sub>buffers-1</sub>(ceil(input pages/buffers)))

## 4: Implementation of DISTINCT

DISTINCT is implemented as a part of the [Project](src/qp/operators/Project.java) Operator, with a new `distinct` flag that decides what the Operator does. 

If distinct is false, Project behaves like the original project, streaming input to output while selecting the requested columns. A difference is in the separation of reading the input buffer and populating the output buffer so that they are not necessarily the same size, a minor bug detailed in [Bugfixes](#Bugfixes)

If distinct is true, Project performs the optimized sort-based duplicate elimination algorithm, largely similar to Sort. It first generates sorted runs where tuples only contain projected attributes (smaller than input tuples) and the tuples are sorted based on all projected attributes. Then in the merging phase while scanning the `inBuffers` for the smallest tuple to place in the output buffer, the previous inserted tuple is tracked and if duplicates are found while scanning the input buffer, they are discarded.

Similar to Sort, Project with distinct=true is also not streaming, thus consuming all the base operator's input upon open().

The cost and output size of distinct can be found in [PlanCost.java](src/qp/optimizer/PlanCost.java). Under the assumption of independently distributed attributes, the estimated number of distinct values in the input is given by the product of the number of distinct values of all projected attributes. E.g. if field A has values 1 and 2, and field B has values 'a', 'b', 'c', then `SELECT DISTINCT A, B` will be expected to have 2*3=6 distinct values (1,'a'), (1,'b'), (1,'c'), (2,'a'), (2,'b'), (2,'c'). The size of output is thus the number of distinct values, capped by the size of the input. Cost is largely similar to the Sort Operator, except the number of pages of the input and subsequent passes is different since tuple size may decrease.

## 5: Implementation of GROUPBY

Our group only implemented GROUPBY and not aggregate functions. Thus the only operation that can be done on the grouped table is to select the grouped attributes, which will be distinct. Performing `SELECT (A) ... GROUPBY (B)` is thus equivalent to first projecting distinct B, then projecting A. Since GROUPBY can be expressed in terms of our existing Project operator, we do not need a Groupby Operator. Our implementation is found in [RandomInitialPlan.createProjectOp](src/qp/optimizer/RandomInitialPlan.java), where if the groupby list is nonempty, we create a Project Operator to select distinct tuples by the grouping attributes, then pass the output to another Project Operator with the actual query `projectlist` and `isDistinct`.

Furthermore, plan costs involving GROUPBY are already properly calculated as it simply comprises Project Operators.

We must note that this implementation of GROUPBY only works because aggregate operators are not supported. To add support for aggregate operators, some modifications will be necessary to compute the aggregate values instead of simply discarding duplicates.


## 6: Bugfixes

In the original implementation, if the page size was too small for a single tuple in any result table or intermediate table, the program would enter an infinite loop as Batch has 0 capacity. We fixed this by simply checking the given batch size in the Batch constructor [here](src/qp/utils/Batch.java), and exiting the program if batch size is 0. We deem this an appropriate course of action as there is no way for the program to work around the page size being too small for a single tuple, except if the user were to run the program with a larger input page size.

Another (minor) bug was in the Project Operator. The original operator produced 1 batch of output per input batch. This is not always the case since Projection reduces the size of tuples, thus each Batch can possibly contain more tuples. In fact in the old Project `outbatch` was initialized with the correct (larger) batch size, but it was only filled with `inbatch.size()` tuples. This bug was fixed in the project rewrite above.