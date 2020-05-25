import ipyparallel as ipp

c = ipp.Client()
assert (len(c.ids) == 2)
assert (len(c[:].apply_sync(lambda: "Hello, World")) == 2)