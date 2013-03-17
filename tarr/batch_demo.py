'''
#
  cat > input.csv << EOF
object
man
moon
fish
flower
sun
dog
worm
EOF

#
  python -m tarr.demo input.csv output.csv

#
  cat output.csv
line_num,input,class
1,man,?
2,moon,?
3,fish,ANIMAL
4,flower,PLANT
5,sun,?
6,dog,ANIMAL
7,worm,?

'''

import tarr
from tarr.data import Data
from tarr.language import RETURN_TRUE
import tarr.batch

import unicodecsv


class Reader(tarr.batch.Reader):

    def __init__(self, input_filename):
        self.input_filename = input_filename
        self.file = open(input_filename)
        self.reader = unicodecsv.DictReader(self.file)

    def __iter__(self):
        return self

    def next(self):
        return Data(self.reader.line_num, self.reader.next())

    def close(self):
        self.file.close()


class Writer(tarr.batch.Writer):

    def __init__(self, output_filename):
        self.output_filename = output_filename
        self.file = open(output_filename, 'w')
        self.writer = unicodecsv.DictWriter(
            self.file, fieldnames=u'line_num input class'.split())
        self.writer.writeheader()

    def write(self, data):
        row = {
            u'line_num': data.id,
            u'input': data.payload[u'object'],
            u'class': data.payload[u'class']}
        self.writer.writerow(row)

    def close(self):
        self.file.close()


# transformation

OBJECT_CLASS = {
    u'dog': u'ANIMAL',
    u'cat': u'ANIMAL',
    u'fish': u'ANIMAL',
    u'tree': u'PLANT',
    u'flower': u'PLANT',
    u'computer': u'INANIMATE'
}


@tarr.rule
def classify(data):
    what = data[u'object']
    return {
        u'object': what,
        u'class': OBJECT_CLASS.get(what, u'?')}


PROGRAM = [
    classify,
    RETURN_TRUE
]


class Batch(tarr.batch.TarrBatch):

    def get_reader(self, filename):
        return Reader(filename)

    def get_writer(self, filename):
        return Writer(filename)

    def get_tarr_transform(self):
        return PROGRAM


if __name__ == '__main__':
    import sys
    tarr.batch.main(Batch, sys.argv[1:])
