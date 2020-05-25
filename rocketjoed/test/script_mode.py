import ipyparallel as ipp
c = ipp.Client()
assert (len(c.ids) == 2  )
print(c[:].apply_sync(lambda : "Hello, World"))