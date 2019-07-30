from pyrocketjoe import file_read

file = file_read('big_data.txt')

for i in file.values():
    print(i)
