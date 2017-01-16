import random


class FileGenerator(object):
    def __init__(self, lines_requested, file_name):
        self.lines_requested = lines_requested
        self.file_name = file_name

    def user_generator(self):
        for x in range(0, self.lines_requested):
            yield x

    def age_generator(self, min_age=0, max_age=100):
        for x in range(0, self.lines_requested):
            yield random.randint(min_age, max_age)

    def generate_file(self):
        file = open(self.file_name, "w")
        age_generator = self.age_generator()
        id_generator = self.user_generator()
        for x in range(0, self.lines_requested):
            user_id = next(id_generator)
            age = next(age_generator)
            line = "{},{}".format(user_id, age)
            print(line, file=file)
