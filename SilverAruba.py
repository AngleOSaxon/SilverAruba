import os
from multiprocessing.pool import Pool
import sys


def handle_chunk(filename, chunk_start, chunk_end):
    ages = {}
    with open(filename, mode="r+b") as file:
        file.seek(chunk_start)
        chunk = file.read(chunk_end - chunk_start).decode("utf-8")
        file.close()
        for line in chunk.split("\n"):
            if line.isspace() or line == "":
                continue
            fields = line.split(",")
            if len(fields) != 2:
                raise Exception("File not in expected format")
            age = int(fields[1])
            if age not in ages:
                ages[age] = 0
            ages[age] += 1

    return ages


def find_chunk_ranges(file, desired_size):
    chunk_ranges = []
    start_pos = 0
    file.seek(0)
    chunk = file.read(desired_size).decode("utf-8")
    while len(chunk) != 0:
        chunk_length = len(chunk)
        eof = chunk_length != desired_size
        ends_on_linesep = chunk.endswith("\n")
        if not ends_on_linesep and not eof:
            end_pos = start_pos + chunk.rfind("\n")
        else:
            end_pos = start_pos + desired_size
        chunk_ranges.append((start_pos, end_pos))
        start_pos = end_pos
        file.seek(start_pos)
        chunk = file.read(desired_size).decode("utf-8")

    return chunk_ranges


def get_merge_dictionaries(main):
    def merge_dictionaries(secondary):
        for age_dictionary in secondary:
            for key, value in age_dictionary.items():
                if key in main:
                    main[key] += value
                else:
                    main[key] = value

    return merge_dictionaries


def process_file(filename, debug=False):
    with open(filename, mode="r+b") as file:
        chunk_size = 1024 * 1024 * 100
        file_size = os.path.getsize(filename)
        if chunk_size > file_size:
            chunk_size = file_size
        ranges = find_chunk_ranges(file, chunk_size)

    ages = {}

    pool = Pool()
    call_args = [(filename, range[0], range[1]) for range in ranges]
    result = pool.starmap_async(handle_chunk, call_args, callback=get_merge_dictionaries(ages))
    result.get()

    total_user_count = 0
    for age, count in ages.items():
        print("{},{}".format(age, count))
        total_user_count += count

    if debug:
        print()
        print("Total user count: {}".format(total_user_count))


if __name__ == "__main__":
    process_file(sys.argv[1])
