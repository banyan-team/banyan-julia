G     = 6.67384e-11     # m/(kg*s^2)
dt    = 60*60*24*365.25 # Years in seconds
r_ly  = 9.4607e15       # Lightyear in m
m_sol = 1.9891e30       # Solar mass in kg
b     = 0.0

n = 10
iterations = 10

v_x = zeros((n,n))
v_y = zeros((n,n))
eta = ones((n,n)) # pressure
for i=1:n
	eta[i] = eta[i] * 0.1 * i
end

temps = [ones((n,n)),
		 ones((n,n)),
		 ones((n,n)),
		 ones((n,n))]

for i in 1:iterations
	
end