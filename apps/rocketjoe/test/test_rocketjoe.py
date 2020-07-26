import json
from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
import logging

code = str(
    '''
    print(1)
    '''
)


def test_rocketjoe() -> None:
    with NamedTemporaryFile() as configuration_file:
        configuration_file.write(code.encode())
        configuration_file.flush()

        with Popen(['/app/build/apps/rocketjoe/rocketjoe', configuration_file.name]) as rocketjoe:
            rocketjoe.poll()
            rocketjoe.kill()


logging.basicConfig(level=logging.DEBUG)
test_rocketjoe()
