macro pa(ex...)
    annotation = ex[1:end-1]
    code_region = ex[end]

    print(annotation)
end

@pa {mut x Block(1) y z} where {Cross x 0 y 0} or {mut x Block(1) y z} where {Cross x 0 y 0} begin end