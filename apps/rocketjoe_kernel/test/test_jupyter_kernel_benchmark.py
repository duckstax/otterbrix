# Don't import KernelTests
import jupyter_kernel_test
import jupyter_kernel_mgmt
from jupyter_kernel_mgmt.discovery import KernelFinder
from jupyter_kernel_mgmt.client import BlockingKernelClient

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
