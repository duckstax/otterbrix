print("script start")
import ipyparallel as ipp

print("init client strat")
c = ipp.Client()
print("init client finish")
assert (len(c.ids) == 2)
assert (len(c[:].apply_sync(lambda: "Hello, World")) == 2)
print("script finish")
