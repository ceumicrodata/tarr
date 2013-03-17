from tarr.compiler import Program
from tarr.language import RETURN_TRUE
import contextlib
import os
import multiprocessing
import itertools


# TODO:
# - test main
# - logging of [transform] exceptions
# - convert direct file operations into external operations
# - consider using pyfileseq (show stopper: pyfileseq has no tests (1.0.1))


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

class BatchTransform(object):
    '''Abstract class describing a file transformation

    - how to read input data (get_reader)
    - how to process data (transform)
    - how to write output data (get_writer)
    '''

    def get_reader(self, filename):
        return Reader(filename)

    def get_writer(self, filename):
        return Writer(filename)

    def transform(self, data):
        return data

    def process(self, input_filename, output_filename):
        closing = contextlib.closing
        with closing(self.get_reader(input_filename)) as reader:
            with closing(self.get_writer(output_filename)) as writer:
                for data in iter(reader):
                    writer.write(self.transform(data))


class TarrBatchTransform(BatchTransform):
    '''Abstract class describing a file transformation using
    a TARR transformation

    - how to read input data (get_reader)
    - how to process data (get_tarr_transform)
    - how to write output data (get_writer)
    '''

    def __init__(self):
        self.transformation = Program(self.get_tarr_transform())

    def get_tarr_transform(self):
        # minimal TARR program - do nothing
        return [RETURN_TRUE]

    def transform(self, data):
        try:
            return self.transformation.run(data)
        except Exception:
            return data



def transform_batch(tio):
    # multiprocessing.Pool.map supports one iterable argument
    # so we have to pack and unpack them into/from a tuple
    transformer_class, input, output = tio
    transformer_class().process(input, output)


# file sequence discovery - should match that of csvtools
# consider using pyfileseq for input sequence discovery
# and output sequence generation
# (https://pypi.python.org/pypi/pyfileseq
#  or https://github.com/aldergren/pyfileseq)

def gen_name(prefix, i):
    return prefix + unicode(i)


def count_files_with(prefix):
    count = 0
    while os.path.exists(gen_name(prefix, count)):
        count += 1

    return count


def gen_names(prefix, count):
    for i in xrange(count):
        yield gen_name(prefix, i)


def main(batch_class, arguments):
    # TODO: argparse & help
    # TODO: test
    input, output = arguments
    if os.path.exists(input):
        # single input
        transform_batch((batch_class, input, output))
    else:
        # multiple input -> multiprocessing
        input_count = count_files_with(prefix=input)
        pool = multiprocessing.Pool(maxtasksperchild=1)
        pool.map(
            transform_batch,
            zip(
                itertools.repeat(batch_class),
                gen_names(input, input_count),
                gen_names(output, input_count)),
            chunksize=1)
        pool.terminate()
        pool.join()
