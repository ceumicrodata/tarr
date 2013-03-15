from tarr.compiler import Program
import contextlib


class Reader(object):

    def __init__(self, input_filename):
        pass

    def __iter__(self):
        pass

    def close(self):
        pass


class Writer(object):

    def __init__(self, output_filename):
        pass

    def write(self, data):
        pass

    def close(self):
        pass


# To use multiprocessing all parameters need to be pickleable
# it includes the TARR program, which generally is not pickleable
# so we have to ask users to wrap the program in a top-level function
# or class.
# Users also need to provide custom data readers and writers.

class Batch(object):
    '''Abstract class describing a file transformation

    - how to read input data (get_reader)
    - how to process data (get_program)
    - how to write output data (get_writer)
    '''

    def __init__(self):
        self.transform = Program(self.get_tarr_transform())

    def get_tarr_transform(self):
        raise NotImplementedError

    def get_reader(self, filename):
        return Reader(filename)

    def get_writer(self, filename):
        return Writer(filename)

    def process(self, input_filename, output_filename):
        closing = contextlib.closing
        with closing(self.get_reader(input_filename)) as reader:
            with closing(self.get_writer(output_filename)) as writer:
                for data in iter(reader):
                    try:
                        transformed_data = self.transform.run(data)
                    except Exception:
                        transformed_data = data
                    writer.write(transformed_data)


def main(batch_class, arguments):
    input, output = arguments
    batch_class().process(input, output)
