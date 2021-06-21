G     = 6.67384e-11     # m/(kg*s^2)
dt    = 60*60*24*365.25 # Years in seconds
r_ly  = 9.4607e15       # Lightyear in m
m_sol = 1.9891e30       # Solar mass in kg
b     = 0.0

n = 10

box_size = 1
grid_spacing = 1.0 * box_size / n

iterations = 10

v_x = zeros((n,n))
v_y = zeros((n,n))
eta = ones((n,n)) # pressure
for i=1:n
	eta[i] = eta[i] * 0.1 * i
end

#temps = [ones((n,n)),
#		 ones((n,n)),
#		 ones((n,n)),
#		 ones((n,n))]

tmp = ones((n,n))
du_dt = ones((n,n))
dv_dt = ones((n,n))
tmp1 = ones((n,n))

for i in 1:iterations
	# Compute derivatives with respect to x and y by using difference in z (by shifting) / difference in x or y (going both sides)
	# Derivative of eta with respect to x (second dimension?????!???!!!??)
	roll1 = circshift(eta, (0,-1))
	roll2 = circshift(eta, (0,1))
	tmp1 = (roll1 .- roll2) ./ (grid_spacing * 2.0)

	# Derivative of eta with respect to y (first dimension?????!???!!!??)
	roll1 = circshift(eta, (-1,0))
	roll2 = circshift(eta, (1,0))
	tmp = (roll1 .- roll2) ./ (grid_spacing * 2.0)

	du_dt = -g .* tmp1 - b * u
	dv_dt = -g .* tmp - b .* v

	H = 0
	tmp1 = ((eta .+ H) .* u) 
	tmp2 = tmp 1 .* v

	roll1 = circshift(tmp1, (0,-1))
	roll2 = circshift(tmp1, (0,1))
	tmp1 = -1 * (roll1 .- roll2) ./ (grid_spacing * 2.0)

	roll1 = circshift(tmp, (0,-1))
	roll2 = circshift(tmp, (0,1))
	tmp = tmp1 .- ((roll1 .- roll2) ./ (grid_spacing * 2.0))

	# On to line 106
end