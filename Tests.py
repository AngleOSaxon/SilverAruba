import DataGenerator as gen
import SilverAruba as handler
import sys


if __name__ == "__main__":
    length = int(sys.argv[1])
    filename = "testdata.txt"
    if len(sys.argv) > 2:
        filename = sys.argv[2]
    file_gen = gen.FileGenerator(length, filename)
    file_gen.generate_file()
    handler.process_file(filename, True)