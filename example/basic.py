def main():
    with smart_open("big_data.txt") as file:
        for i in file:
            print(i)


if __name__ == '__main__':
    main()



