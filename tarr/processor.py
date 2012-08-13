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

    @classmethod
    def instantiate(cls, container):
        processor = cls()
        processor.container = container
        return processor

    def process(self, data):
        ''' returns the processed data

        this is the method to be overridden in subclasses
        '''
        raise NotImplementedError


# decorators that create Processor-s from functions

class BranchProcessor(Processor):

    func = None

    def process(self, data):
        if self.func(data):
            return data
        raise ProcessorFailed


class RuleProcessor(Processor):

    func = None

    def process(self, data):
        return self.func(data)


def func_instantiator(cls, func):
    def instantiate(container):
        processor = cls.instantiate(container)
        processor.func = func
        return processor
    return instantiate


# Test from Test And Rule Registry, not named `test` to avoid test runner picking it up
def branch(func):
    '''
    # branching construct
    @test
    def odd(data):
        return data.number % 2 == 1
    '''
    func.instantiate = func_instantiator(BranchProcessor, func)
    return func


# Rule from Test And Rule Registry
def rule(func):
    '''
    # transformation construct
    @rule
    def transform(data):
        ...
        return new_data
    '''
    func.instantiate = func_instantiator(RuleProcessor, func)
    return func
