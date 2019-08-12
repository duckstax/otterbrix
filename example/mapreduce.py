from pyrocketjoe import file_read
from pyrocketjoe.MapReduce import to_stream, Application, Context, Stream, DataSource

app = Application()


def helper_map(context: Context, stream: Stream):
    pass


def helper_reduce(context: Context, stream: Stream):
    pass


def helper_to_stream(context: Context, data_source: DataSource):
    pass


@app.task()
def pipeline():
    file = file_read("big_file.txt", "r")

    counts = file.to_stream(helper_to_stream).map(helper_map).reduce(helper_reduce)

    counts.save_text_file("result.txt")


if __name__ == "__main__":
    pass
