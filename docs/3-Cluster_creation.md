# Create a cluster

“include” - list of other Banyanfile’s to include, i.e., other “banyanfile” dicts
“banyanfile”
“language” - “jl” or “py”
“cluster”
“scripts” - list of strings where each string is a command to execute on the cluster
“packages” - list of packages to Pkg.install
“pt_lib_info” - dict
“pt_lib” - path name for pt_lib.jl
“job”
“code” - list of strings to insert at start

```json
{
    "include": [],
    "require": {
        "language": "jl"|"py",
        "cluster": {
            "commands": ["string",],
            "packages": ["string",],
            "pt_lib_info": "string",
            "pt_lib": "string"
        },
        "job": []
    }
}
```
* **include** (list)  
List of paths to other Banyanfiles to include, or the actual Banyanfile dictionaries
* **require** (dict)
  * **language** (string)  
  Language used. Currently supporting Julia ("jl")
  * **cluster** (dict)
    * **commands** (list)  
    List of commands to execute on creation of cluster
    * **packages** (list)  
    List of language-dependent packages to install
    * **pt_lib_info** (dict or string)  
    Path to pt_lib_info json file or actual pt_lib_info dict
    * **pt_lib** (string)  
    Optional path to pt_lib
  * **jobs** (list)  
  List of lines to code to be executed on creation of a job in this cluster
