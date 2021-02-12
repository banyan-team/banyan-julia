import matplotlib.pyplot as plt

num_workers = [1, 2, 4, 8, 16]

wo_cache_optim = [237.947, 163.376, 85.370, 42.892, 22.544]
w_cache_optim = [103.526, 83.300, 41.891, 21.776, 10.871]

plt.plot(num_workers, wo_cache_optim, label="Cache Optimized")
plt.plot(num_workers, w_cache_optim)  #, label="w/o cache optimization")
plt.title("Runtime for Black Scholes")
plt.xlabel("Number of Workers")
plt.ylabel("Runtime (s)")
plt.legend()
plt.grid(alpha=0.5)
plt.savefig("blackscholes_runtime.png", dpi=200)