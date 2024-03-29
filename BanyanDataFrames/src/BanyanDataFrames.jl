module BanyanDataFrames

using Arrow,
    Banyan,
    BanyanArrays,
    DataFrames,
    Dates,
    Downloads,
    FileIO,
    FilePathsBase,
    Missings,
    MPI,
    ProgressMeter,
    Random,
    Requires,
    Serialization,
    Statistics

# Types
export DataFrame, GroupedDataFrame

# I/O
export read_csv, write_csv, read_parquet, write_parquet, read_arrow, write_arrow

# Dataframe properties
export nrow, ncol, size, names, propertynames

# Dataframe filtering
export dropmissing, filter, unique, nonunique

# Dataframe selection and column manipulation
export getindex, setindex!, rename

# Dataframe sorting
export sort

# Dataframe joining
export innerjoin

# Grouped dataframe properties
export length, groupcols, valuecols

# Grouped dataframe methods
export groupby, select, transform, combine, subset

# Missing
export allowmissing, disallowmissing

# PFs
export ReadBlockCSV,
    ReadBlockParquet,
    ReadBlockArrow,
    ReadBlockBalancedCSV,
    ReadBlockBalancedParquet,
    ReadBlockBalancedArrow,
    ReadGroupCSV,
    ReadGroupParquet,
    ReadGroupArrow,
    WriteParquet,
    WriteCSV,
    WriteArrow,
    CopyFromArrow,
    CopyFromCSV,
    CopyFromParquet,
    CopyToCSV,
    CopyToParquet,
    CopyToArrow,
    CopyTo,
    SplitBlock,
    SplitGroup,
    RebalanceDataFrame,
    ConsolidateDataFrame,
    ShuffleDataFrame,
    ReturnNullGrouping,
    ReturnNullGroupingConsolidated,
    ReturnNullGroupingRebalanced,
    ReduceDataFrame,
    ReduceAndCopyToArrow,
    add_sizes

# Locations
export RemoteTableSource, RemoteTableDestination

include("pt_lib_constructors.jl")
include("locations.jl")
include("df.jl")
include("gdf.jl")
include("utils_pfs.jl")
include("pfs.jl")

# We can include arrow.jl because we anyway need the Arrow.jl package for pfs.jl
include("arrow.jl")

function __init__()
    @require CSV = "336ed68f-0bac-5ca0-87d4-7b16caf5d00b" include("csv.jl")
    @require Parquet = "626c502c-15b0-58ad-a749-f091afb673ae" include("parquet.jl")
end

if Base.VERSION >= v"1.4.2"
    include("precompile.jl")
    _precompile_()
end

end # module
