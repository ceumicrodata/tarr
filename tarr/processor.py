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