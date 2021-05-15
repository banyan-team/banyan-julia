# TODO: Delete this file because the implementations of AbstractSampleWithKeys
# should be in the nnotated libraries.

function compute_quantiles(fut::AbstractFuture, key)
    key = hash(string(key))
    get!(sample.sample_properties, :quantiles) do Dict() end
    get!(sample.sample_properties[:quantiles], key) do
        df = compute_sample(fut)
        max_ngroups = compute_max_ngroups(fut, key)
        ngroups = min(max_ngroups, get_job().nworkers, 128)
        data = sort(getproperty(df, key))
        datalength = length(data)
        grouplength = div(datalength, ngroups)
        [
            [
                (i-1)*grouplength + 1,
                if i == ngroups
                    datalength
                else
                    i*grouplength
                end
            ]
            for i in 1:ngroups
        ]
    end
end

function compute_max_ngroups(fut::AbstractFuture, key=nothing, axis=nothing)
    key = hash(string(key))
    get!(sample.sample_properties, key) do Dict() end
    get!(sample.sample_properties[:max_ngroups], key) do
        df = compute_sample(fut)
        # The maximum # of groups is the inverse of the relative frequency of
        # the mode
        nrow(df) / maximum(combine(groupby(df, key), nrow).nrow)
    end
end

function compute_size(fut::AbstractFuture)
    get!(sample.sample_properties, :size) do
        Base.summarysize(compute_sample(fut))
    end
end
