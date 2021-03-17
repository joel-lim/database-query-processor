# On Query Processing
## 1: Implementation of Block Nested Loops Join

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

## 3: Implementation of DISTINCT

## 4: Implementation of ORDERBY

## 5: Implementation of GROUPBY

## 6: Bugfixes
