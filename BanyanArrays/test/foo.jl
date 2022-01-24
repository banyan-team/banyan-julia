struct Foo
    x::String
end

function new_foo(foo::Foo)
    Foo(foo.x * "100")
end