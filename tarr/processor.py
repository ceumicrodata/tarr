class ProcessorFailed(Exception):
    pass


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

    def process(self, data):
        ''' returns the processed data

        this is the method to be overridden in subclasses
        '''
        raise NotImplementedError


# decorators that create Processor-s from functions

# Test from Test And Rule Registry, not named `test` to avoid test runner picking it up
def branch(func):
    '''
    # branching construct
    @test
    def odd(data):
        return data.number % 2 == 1
    '''
    class wrapper(Processor):

        def process(self, data):
            if func(data):
                return data
            raise ProcessorFailed

    wrapper.__name__ = func.__name__
    return wrapper


# Rule from Test And Rule Registry
def rule(func):
    '''
    # transformation construct
    @rule
    def transform(data):
        ...
        return new_data
    '''
    class wrapper(Processor):

        def process(self, data):
            return func(data)

    wrapper.__name__ = func.__name__
    return wrapper
