![ubuntu 16.04](https://github.com/cyberduckninja/RocketJoe/workflows/ubuntu%2016.04/badge.svg)

![ubuntu 18.04](https://github.com/cyberduckninja/RocketJoe/workflows/ubuntu%2018.04/badge.svg)

![ubuntu 20.04](https://github.com/cyberduckninja/RocketJoe/workflows/ubuntu%2020.04/badge.svg)

RocketJoe is a software development platform for creating high-performance applications. 

Scales and speeds up data-intensive apps by using distributed computing architecture. 

It improves analytical algorithms’ performance and provides customization options for setting up work with data.


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

conan remote add bincrafters https://api.bintray.com/conan/bincrafters/public-conan

```
### for debian base

```bash

apt install git ccache g++ cmake python3 python3-dev python3-pip ninja-build

pip3 install conan --upgrade

pip3 install cmake

conan remote add cyberduckninja https://api.bintray.com/conan/cyberduckninja/conan

conan remote add bincrafters https://api.bintray.com/conan/bincrafters/public-conan
 
```
Hint: on Ubuntu 16.04 (and may be some another) you should install actual cmake manually.

Also you should install Celery:
```
pip3 install celery
```

### Build 

```bash

git clone https://github.com/cyberduckninja/RocketJoe.git rocketjoe

cd rocketjoe

conan install . --build=boost

mkdir build

cd build

cmake .. -GNinja -DCMAKE_BUILD_TYPE=Release

cmake --build .

./apps/rocketjoe/rocketjoe 
 
```

### Тестирование 

1.
Скачать ветку:
```
git clone https://github.com/maslenitsa93/RocketJoe -b fix-readme-and-workers

```

2.
Этапы с Docker не выполнять. Установить все необходимые зависимости:
https://github.com/maslenitsa93/RocketJoe/tree/fix-readme-and-workers#setup-developers-environments
Тестировала на Ubuntu 20 и Python 3.8.

3.
Собрать по инструкции https://github.com/maslenitsa93/RocketJoe/tree/fix-readme-and-workers#build
На Ubuntu 20 использовала родной cmake.

4. В папке build создать стандартный тестовый файл tasks.py
```
from celery import Celery

app = Celery('tasks', broker='pyamqp://guest@localhost//')

@app.task
def writeconsole(msg):
    print(msg)
```

5. Запустить воркер
```
rocketjoe_worker tasks.py
```

6. Отдельно запустить Python в папке, где лежит tasks.py, и выполнить следующие строки:
```
from tasks import add
add.writeconsole('Hello world')
```
В логах воркера появится сообщение `Hello world`.

7. Настройка лимитов возможна с помощью файла конфигурации Celery
https://docs.celeryproject.org/en/stable/userguide/workers.html

8. В случае с этими тест-кейсами
https://github.com/Sibuken/celery_test_cases
нужно содержимое папки app положить рядом с rocketjoe_worker (бинарником) и запустить его, как и в пункте 5. Затем делать вызовы аналогично пункту 6.
