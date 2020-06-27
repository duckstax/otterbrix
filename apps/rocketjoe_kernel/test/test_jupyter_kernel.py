# Don't import KernelTests
import jupyter_kernel_test
import jupyter_kernel_mgmt
from jupyter_kernel_mgmt.discovery import KernelFinder
from jupyter_kernel_mgmt.client import BlockingKernelClient


class RocketJoePythonTests(jupyter_kernel_test.KernelTests):
    kernel_name = 'rocketjoe'
    language_name = 'python'
    file_extension = '.py'
    code_hello_world = 'print(\'hello, world\')'
    code_stderr = 'import sys; print(\'test\', file = sys.stderr)'
    completion_samples = [{
        'text': 'zi', 'matches': {'zip'},
    }]
    complete_code_samples = ['1', 'print(\'hello, world\')', 'def f(x):\n  return x * 2\n']
    incomplete_code_samples = ['print(\'\'\'hello', 'def f(x):\n  x*2']
    invalid_code_samples = ['import = 7q']
    # code_page_something = 'zip?'
    code_generate_error = 'raise'
    code_execute_result = [
        {'code': '1 + 2 + 3', 'result': '6'},
        {'code': '[n * n for n in range(1, 4)]', 'result': '[1, 4, 9]'}
    ]
    # code_display_data = [
    #    {'code': 'from IPython.display import HTML, display; display(HTML(\'<b>test</b>\'))',
    #     'mime': "text/html"},
    #    {'code': 'from IPython.display import Math, display; display(Math(\'\\frac{1}{2}\'))',
    #     'mime': 'text/latex'}
    # ]
    code_history_pattern = '1?2*'
    # supported_history_operations = ('tail', 'search')
    code_inspect_sample = 'zip'


def kernel_info_request(kc) -> None:
    kc.kernel_info()


def execute_request_1(kc) -> None:
    kc.execute_interactive("i = 0\ni + 1")


def execute_request_2(kc) -> None:
    kc.execute_interactive("print(6 * 7)")


def complete_request_1(kc) -> None:
    kc.complete("prin")


def complete_request_2(kc) -> None:
    kc.complete("print")


def is_complete_request_1(kc) -> None:
    kc.is_complete("prin")


def is_complete_request_2(kc) -> None:
    kc.is_complete("print")


def test_rocketjoe_kernel_info_request(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(kernel_info_request, kc)


def test_rocketjoe_kernel_info_request(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(kernel_info_request, kc)


def test_rocketjoe_execute_request_1(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(execute_request_1, kc)


def test_rocketjoe_execute_request_1(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(execute_request_1, kc)


def test_rocketjoe_execute_request_2(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(execute_request_2, kc)


def test_rocketjoe_execute_request_2(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(execute_request_2, kc)


def test_rocketjoe_complete_request_1(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(complete_request_1, kc)


def test_rocketjoe_complete_request_1(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(complete_request_1, kc)


def test_rocketjoe_complete_request_2(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(complete_request_2, kc)


def test_rocketjoe_complete_request_2(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(complete_request_2, kc)


def test_rocketjoe_is_complete_request_1(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(is_complete_request_1, kc)


def test_rocketjoe_is_complete_request_1(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(is_complete_request_1, kc)


def test_rocketjoe_is_complete_request_2(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(is_complete_request_2, kc)


def test_rocketjoe_is_complete_request_2(benchmark) -> None:
    with jupyter_kernel_mgmt.run_kernel_blocking('spec/rocketjoe') as kc:
        benchmark(is_complete_request_2, kc)
