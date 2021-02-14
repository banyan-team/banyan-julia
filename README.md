# banyan-julia

## Testing

```cmd
NWORKERS=4 SSH_KEY_PAIR=~/Downloads/EC2ConnectKeyPair.pem julia --project=.
```

```cmd
NWORKERS_ALL=true SSH_KEY_PAIR=~/Downloads/EC2ConnectKeyPair.pem julia --project=.
```

```julia
using Pkg; Pkg.test("Banyan", test_args=["scholes"])
```