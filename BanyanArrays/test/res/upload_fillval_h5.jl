using HDF5

original = h5open(download("https://support.hdfgroup.org/ftp/HDF5/examples/files/exbyapi/h5ex_d_fillval.h5"))
new = h5open(joinpath(homedir(), ".banyan/mnt/s3/banyan-cluster-data-pumpkincluster0-3e15290827c0c584/fillval.h5"), "w")

new["DS1"] = repeat(original["DS1"][:,:], 100, 100)

close(new)
close(original)