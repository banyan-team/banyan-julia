using HTTP
using JSON

##############
# CONVERSION #
##############

jl_to_json(j) = JSON.json(j)

json_to_jl(j) = JSON.parse(j)


#########################
# ENVIRONMENT VARIABLES #
#########################

is_debug_on() = "JULIA_DEBUG" in keys(ENV) && ENV["JULIA_DEBUG"] == "all"

macro in_env(key)
    return :(string("BANYAN_", getpid(), "_", $key) in keys(ENV))
end

macro env(key)
    return :(ENV[string("BANYAN_", getpid(), "_", $key)])
end

macro delete_in_env(key)
    return :(delete!(ENV, string("BANYAN_", getpid(), "_", $key)))
end


################
# API REQUESTS #
################

method_to_string(method) = begin
    if method == :create_cluster
        "create-cluster"
    elseif method == :destroy_cluster
        "destroy-cluster"
    elseif method == :create_job
        "create-job"
    elseif method == :destroy_job
        "destroy-job"
    elseif method == :evaluate
        "evaluate"
    end
end

"""
Sends given request with given content
"""
function send_request_get_response(method, content::Dict{String,Any})
    # Prepare request
    # TODO: Remove adding secret token
    content["secret_token"] = SECRET_TOKEN
    content["debug"] = is_debug_on()
    # TODO: Use something other than delayedrequest.com
    url = string(BANYAN_API_ENDPOINT, method_to_string(method))
    headers = (("content-type", "application/json"))

    # Post and return response
    try
        response = HTTP.post(url, headers, JSON.json(content))
        return JSON.parse(String(response.body))
    catch e
        if isa(e, HTTP.ExceptionRequest.StatusError)
            if (e.response.status != 504)
		#println(e.response.body)
		#println(String(take!(IOBuffer(e.response.body))))
                throw(ErrorException(String(take!(IOBuffer(e.response.body)))))
            elseif method == :create_cluster
                println("Cluster creation in progress. Please check dashboard to view status.")
            elseif method == :create_job
                println("Job creation in progress. Please check dashboard to view status.")
            elseif method == :evaluate
                println("Evaluation is in progress. Please check dashboard to view status.")
            end
        else
            rethrow()
        end
    end

end
