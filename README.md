![ubuntu 16.04](https://github.com/cyberduckninja/RocketJoe/workflows/ubuntu%2016.04/badge.svg)

![ubuntu 18.04](https://github.com/cyberduckninja/RocketJoe/workflows/ubuntu%2018.04/badge.svg)

![ubuntu 20.04](https://github.com/cyberduckninja/RocketJoe/workflows/ubuntu%2020.04/badge.svg)

RoсketJoe is a Python worker pool for distributed execution of computational tasks on a CPU or GPU. 
RocketJoe is intended for splitting two-dimensional dataframe into chunks for processing large volumes of data on a cluster of machines or accelerators, such as GPUs. 
It is also integrated with Jupyter Notebook for the possibility of interactive work with data and algorithms, as well as their visualization.

# Roadmap
- [ ] Integration of the Jupyter Kernel Protocol to simplify data scientists' processing and visualization of data, modeling of any species possible in the Jupyter ecosystem.
   - [x] Implementation of the protocol for the execution, completion and introspection of the code.
   - [ ] Integration of ipyparallel for parallel execution in a cluster.
   - [x] Storage of cPickle in shared memory to minimize copying in the the process pool.
- [ ] Monitoring: implementation of monitoring capabilities for the state of the worker pool and processes, visualization of statistics and load.

## Environment Settings Data Engineer or Data Scientist for Jupyter Notebook 

### Requirements
* Docker (18.09.5 tested)
* Docker Compose (1.24.1 tested)

### Build and start
1. Clone repository:
```bash
git clone https://github.com/cyberduckninja/RocketJoe.git
cd RocketJoe
```

2. Run RocketJoe Kernel and Jupyter Notebook
```bash
docker-compose -f docker/docker-compose-jupyter.yaml up
```

3. Open the browser at http://localhost:8888/?token=your_token, where your_token
is the access token for the Jupyter Notebook, which will be displayed in the
logs when the container starts.


## Setup Developers Environments 

### for mac os x 

```bash

brew install cmake lua ccache conan python3 libiconv

conan remote add cyberduckninja https://api.bintray.com/conan/cyberduckninja/conan

```
### for debian base

```bash

apt install git ccache g++ cmake python3 python3-dev python3-pip

pip3 install conan --upgrade

conan remote add cyberduckninja https://api.bintray.com/conan/cyberduckninja/conan
 
```

### Build 

```bash

git clone https://github.com/cyberduckninja/RocketJoe.git rocketjoe

cd rocketjoe

mkdir build

cd build

cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release

cmake --build .

./rocketjoe 
 
```
