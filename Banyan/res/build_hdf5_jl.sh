# NOTE: Here we check if both HDF5 and HDF5.jl are built. If Julia was
# re-installed, HDF5.jl maybe needs to be reinstalled so that it can
# be linked with the new MPI that was setup.
if [[ ! -d ~/hdf5 ]] || [[ $(julia/bin/julia --color=no -e "import Pkg; print(Pkg.dependencies())" | grep -c "HDF5") == 0 ]]
then
    # Download HDF5 source, unzip, and cd
    wget https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.12/hdf5-1.12.1/src/hdf5-1.12.1.tar.gz
    tar -xvzf hdf5-1.12.1.tar.gz
    rm hdf5-1.12.1.tar.gz
    cd hdf5-1.12.1

    # Install HDF5 in ~/hdf5
    mkdir ~/hdf5
    # TODO: Maybe we can configure this to only produce ~/hdf5/lib because
    # maybe that's all we need
    ./configure --enable-parallel --prefix=/home/ec2-user/hdf5
    make install

    # Clean up and build HDF5.jl
    # TODO: Determine if we actually ever reinstall MPI. We might just be
    # reinstalling Julia and so we wouldn't need to re-install HDF5 linked with
    # MPI; we would just need to re-add and re-build the package.
    cd -
    rm -rf ~/hdf5-1.12.1
    JULIA_HDF5_LIBRARY_PATH=~/hdf5 julia/bin/julia -e "import Pkg; Pkg.add(\"HDF5\"); Pkg.build(\"HDF5\")"
fi