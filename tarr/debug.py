# drop data to file filter
import tarr.compiler_base


def format_data(data):
    return '{0.id}: {0.payload}'.format(data)


class WRITE_TO_FILE(tarr.compiler_base.Instruction):

    @property
    def __name__(self):
        return 'POINT OF INTEREST - WRITE("{}")'.format(self.filename)

    def __init__(self, filename, formatter=format_data):
        self.format = formatter
        self.filename = filename

    def run(self, runner, data):
        # NOTE: we need to do writing in UNBUFFERED mode (buffering=0)
        # as potentially there are other processes writing to the same file *NOW*
        with open(self.filename, 'ab', buffering=0) as f:
            f.write(self.format(data) + '\n')
        return data

    def clone(self):
        return self.__class__(filename=self.filename, formatter=self.format)
