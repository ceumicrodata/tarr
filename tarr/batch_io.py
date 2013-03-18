import tarr.batch
from tarr.data import Data
from tarr.payload import New as new_payload
import unicodecsv
import collections # namedtuple
import operator

# FIXME: add tests


def make_extractor(result_classname, fields, accessors):
    cls = collections.namedtuple(result_classname, fields)
    extractors = tuple(accessors[name] for name in fields)

    def extract_record(row):
        return cls(*(extractor(row) for extractor in extractors))

    return extract_record


class TarrCsvReader(tarr.batch.Reader):

    def __init__(self, id_fields, payload_fields, input_filename):
        '''Read a CSV file as a sequence of tarr.Data objects.

        id_fields: field names, these will go into data.id
        payload_fields: field names, these will go into data.payload.input
        '''

        self.input_filename = input_filename
        self.file = open(input_filename)
        self.reader = unicodecsv.reader(self.file)
        header = self.reader.next()
        accessors = dict(
            (header[i], operator.itemgetter(i))
            for i in xrange(len(header)))
        self.extract_id = (
            make_extractor(
                'Id', id_fields, accessors))
        self.extractor_payload = (
            make_extractor(
                'Input', payload_fields, accessors))

    def __iter__(self):
        return self

    def next(self):
        row = self.reader.next()
        id = self.extract_id(row)
        payload = self.extractor_payload(row)
        return Data(id, new_payload(payload))

    def close(self):
        self.file.close()


class CsvWriter(tarr.batch.Writer):

    def __init__(self, field_extractors, output_filename):
        '''Convert data objects to CSV file

        field_extractors: sequence of (field name, extractor) pairs
        '''
        self.field_extractors = tuple(field_extractors)
        self.extractors = [
            extractor
            for field, extractor in self.field_extractors]

        self.output_filename = output_filename
        self.file = open(output_filename, 'w')
        self.writer = unicodecsv.writer(self.file)

        header = [field for field, extractor in self.field_extractors]
        self.writer.writerow(header)

    def write(self, data):
        self.writer.writerow(
            [extractor(data) for extractor in self.extractors])

    def close(self):
        self.file.close()
