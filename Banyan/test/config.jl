@testset "Configure using $method" for method in [
    "function arguments", "environment variables", "toml file"
]
    user_id = "user1"
    api_key = "12345"

    if method == "function arguments"
        config = configure(user_id=user_id, api_key=api_key; banyanconfig_path="tempfile.toml")
    elseif method == "environment variables"
        ENV["BANYAN_USER_ID"] = user_id
        ENV["BANYAN_API_KEY"] = api_key
        config = configure(; banyanconfig_path="tempfile.toml")
    elseif method == "toml file"
        delete!(ENV, "BANYAN_USER_ID")
        delete!(ENV, "BANYAN_API_KEY")
        config = configure(; banyanconfig_path="tempfile.toml")
    end
    println(config)
    @test config["banyan"]["user_id"] == user_id && config["banyan"]["api_key"] == api_key

    rm("tempfile.toml", force=true)
end