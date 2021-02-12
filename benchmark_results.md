# Benchmarking Results

## Black Scholes

With Banyan
- 1
  - size=1e9, 
  - dim_0=9, dim_1=1
  - 42.442 s (10716 allocations: 33.53 GiB)
  - slurm job 584


The following table shows the runtime for Black Scholes.

| Num Workers | Sequential | Parallelized Without Cache Optimization | Parallelized With Cache Optimization |
| :---: | :---: | :---: | :---: |
| 1 |  |  | | |
| 2 | __ |  | | | 
| 4 | __ |  | | |
| 8 | __ |  | | |
| 16 | __ |  | |


## Matrix Multiplication

The following table shows the runtime (s) for matrix multiplication A x B, averaged over 5 trials, where the dimensions of A and B are (1500, 10,000) and (10,000, 5000) respectively.

| Num Workers | Sequential | Parallelized Without Cache Optimization | Parallelized With Cache Optimization |
| :---: | :---: | :---: | :---: |
| 1 | 40.920 | 31.543 | 31.956 |
| 2 | __ | 26.172 | 28.699 | 
| 4 | __ | 13.388 | 14.735 |
| 8 | __ | 7.380 | 6.829 |
| 16 | __ | 3.964 | 3.951 |


The following table shows the slurm job id, for reference.

| Num Workers | Sequential | Parallelized Without Cache Optimization | Parallelized With Cache Optimization |
| :---: | :---: | :---: |
| 1 | 598 | 603 |
| 2 | 597 | 602 |
| 4 | 596 | 601 |
| 8 | 593 | 600 |
| 16 | 592 | 599 |

