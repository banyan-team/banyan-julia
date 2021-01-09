data = Future()
x = Future()
res = Future()

@src data HDF5("my_data.h5")

# Creates a PartitioningConstraint
crossed = Cross(data, other)
crossed = Cross((data, 0), (data, 1))

# Creates a Task
simple_pa = @pa mut data Block() x Value() where [crossed, ordered]

# Merges Tasks together by merging their PA unions
@pp [simple_pa, unsorted_pa] begin
    res = data .* x
end

@dst data S3(".....")