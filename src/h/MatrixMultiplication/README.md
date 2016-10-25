# Matrix Multiplication Using MapReduce.

The input file has one line for each non-zero element of a matrix A: Amn with following format:

A,m,n,A_nm

A: the name of matrix
m: row of the matrix
n: column of the matrix
A_nm: value of the element 

The output file will only include non-zero element of result matrix R: Rmn

m,n,r_nm
m: row of the matrix
n: column of the matrix
r_nm: value of the element

This implement tried to solve one task by each MapReduce, and used 3 MapReduce Jobs to solve the problem. 

When m by n matrix A multiply n by p matrix B, the result will be a m by p matrix. Because the input for matrixes only include non-zero elements. So the matrixes could be any dimension include those input elements. We only need m and p to get the result, and don't need worry about n. The elements of A will be used by p times, and the elements of B will be used by m times.

Task one: Get m and p from input file.
Task two: multiply elements.
Task three: sum of values.


