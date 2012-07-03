class Processor(object):
    '''
    Base class for data processors.
    '''

    # '''
    # User visible description of what it does, please define it on subclasses
    # '''

    container = None

    def __init__(self, container):
        self.container = container

    def fail(self):
        self.container.fail()

    def need_human(self):
        self.container.need_human()

    def process(self, data):
        ''' returns the processed data

        this is the method to be overridden in subclasses
        '''
        raise NotImplementedError


def processor_function(func):
    class wrapper(Processor):

        def process(self, data):
            return func(data)

    wrapper.__name__ = func.__name__
    return wrapper
