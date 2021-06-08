# Expected Output According to Reference python : an array of 4841.08061686
lat_ref = 0.7098
lon_ref = 1.2390

lat_test = ones(10) * 0.069
lon_test = ones(10) * 0.069

dlat = sin.((lat_test .- lat_ref) / 2) .^ 2
dlon = sin.((lon_test .- lon_ref) / 2) .^ 2

a = cos.(lat_ref) * cos.(lat_test) .* dlon + dlat

c = asin.(sqrt.(a)) * 2 * 3959.0 # 3959.0 is miles conversion I think
print(c)