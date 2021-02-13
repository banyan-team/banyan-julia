import matplotlib.pyplot as plt

num_workers = [1, 2, 4, 8, 16]

#################
# BLACK SCHOLES #
#################

wo_cache_optim = [237.947, 163.376, 85.370, 42.892, 22.544]

w_l2cache_optim = [103.517, 71.561, 36.956, 18.404, 9.756]
w_l1cache_optim = [93.652, 81.079, 40.485, 19.570, 10.368]

plt.plot(num_workers, wo_cache_optim, marker='o', label="Without Cache Optimization")  #, label="w/o cache optimization")
plt.plot(num_workers, w_l2cache_optim, marker='o', label="L2 Cache Optimized")
plt.plot(num_workers, w_l1cache_optim, marker='o', label="L1 Cache Optimized")
plt.title("Runtime for Black Scholes")
plt.xlabel("Number of Workers")
plt.ylabel("Execution Runtime (s)")
plt.yscale("log")
plt.legend()
plt.grid(alpha=0.5)
plt.savefig("blackscholes_runtime.pdf", dpi=200)
plt.close()



##########
# MATMUL #
##########

wo_cache_optim = [31.543, 26.172, 13.388, 7.380, 3.964]
w_cache_optim = [96.275, 68.332, 37.093, 18.993, 10.289]


plt.plot(num_workers, wo_cache_optim, marker='o', label="Without Cache Optimization")  #, label="w/o cache optimization")
plt.plot(num_workers, w_cache_optim, marker='o', label="L2 Cache Optimized")
plt.title("Runtime for Matrix Multiplication")
plt.xlabel("Number of Workers")
plt.ylabel("Execution Runtime (s)")
plt.yscale("log")
plt.legend()
plt.grid(alpha=0.5)
plt.savefig("matmul_runtime.pdf", dpi=200)
plt.close()
