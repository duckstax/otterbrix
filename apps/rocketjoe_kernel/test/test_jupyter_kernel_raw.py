import socket
from contextlib import closing
import json
from tempfile import NamedTemporaryFile
from subprocess import Popen, PIPE
import logging

import zmq
from aiozmq import create_zmq_stream

import asyncio


def find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(('', 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        return sock.getsockname()[1]


async def test_kernel() -> None:
    transport = 'tcp'
    ip = '127.0.0.1'
    shell_port = find_free_port()
    control_port = find_free_port()
    stdin_port = find_free_port()
    iopub_port = find_free_port()
    heartbeat_port = find_free_port()
    configuration = {
        'transport': transport,
        'ip': ip,
        'shell_port': shell_port,
        'control_port': control_port,
        'stdin_port': stdin_port,
        'iopub_port': iopub_port,
        'hb_port': heartbeat_port,
        'key': 'a7d9f4f3-acad37f08d4fe05069a03422',
        'signature_scheme': 'hmac-sha256'
    }
    configuration_json = json.dumps(configuration)

    with NamedTemporaryFile() as configuration_file:
        configuration_file.write(str.encode(configuration_json))
        configuration_file.flush()

        with Popen(['/app/build/apps/rocketjoe_kernel/rocketjoe_kernel', '--jupyter_connection', configuration_file.name]) as rocketjoe:
            shell_stream = await create_zmq_stream(
                zmq.DEALER,
                connect=f'{transport}://{ip}:{shell_port}'
            )
            control_stream = await create_zmq_stream(
                zmq.DEALER,
                connect=f'{transport}://{ip}:{control_port}'
            )
            stdin_stream = await create_zmq_stream(
                zmq.DEALER,
                connect=f'{transport}://{ip}:{stdin_port}'
            )
            iopub_stream = await create_zmq_stream(
                zmq.SUB,
                connect=f'{transport}://{ip}:{iopub_port}'
            )

            iopub_stream.transport.subscribe(b'')

            heartbeat_stream = await create_zmq_stream(
                zmq.REQ,
                connect=f'{transport}://{ip}:{heartbeat_port}'
            )

            rocketjoe.poll()

            if rocketjoe.returncode is None:
                #logging.info(await iopub_stream.read())
                shell_stream.write([
                    b'<IDS|MSG>',
                    b'bd4f43215c9d72a4cb53599fd054d5c4cc21d6d0a6654fe4fe5aa0767637500c',
                    b'{"msg_id":"59435563b60d4158a5e96614bdcfb9f7_0","msg_type":"kernel_info_request","username":"username","session":"59435563b60d4158a5e96614bdcfb9f7","date":"2020-07-08T17:21:46.644570Z","version":"5.3"}',
                    b'{}',
                    b'{}',
                    b'{}'
                ])

                raw_datas = await shell_stream.read()
                data = [raw_data.decode('ascii') for raw_data in raw_datas]
                header = json.loads(data[2])
                parent_header = json.loads(data[3])
                metadata = json.loads(data[4])
                content = json.loads(data[5])

                for key in ['date', 'msg_id', 'session', 'username', 'version']:
                    del header[key]
                    del parent_header[key]

                del content['banner']
                del content['language_info']['version']

                assert header == {'msg_type': 'kernel_info_reply'}
                assert parent_header == {'msg_type': 'kernel_info_request'}
                assert metadata == {}
                assert content == {
                    'help_links': '',
                    'implementation': 'ipython',
                    'implementation_version': 'ipython',
                    'language_info': {
                        'codemirror_mode': {
                            'name': 'ipython',
                            'version': 3
                        },
                        'file_extension': '.py',
                        'mimetype': 'text/x-python',
                        'name': 'python',
                        'nbconvert_exporter': 'python',
                        'pygments_lexer': 'ipython3'
                    },
                    'protocol_version': '5.3'
                }
                rocketjoe.kill()

logging.basicConfig(level=logging.DEBUG)
loop = asyncio.get_event_loop()
loop.run_until_complete(test_kernel())
