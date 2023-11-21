import inspect


class A:
    def __init__(self):
        print("costruttore A")

    def fun_a(self, a, b, c):
        stack = inspect.stack()
        the_class = stack[1][0].f_locals["self"].__class__.__name__
        the_method = stack[1][0].f_code.co_name
        print("I was called by {}.{}()".format(the_class, the_method))
        print(a, b, c)


class B:
    def __init__(self):
        print("costruttore B")
        self.a = A()

    def fun_b(self, a, b, c):
        self.a.fun_a("a", "b", "c")
        print(a, b, c)


class MemoryMap:
    map_ = {}

    @staticmethod
    def hget(*args, defval=None):
        try:
            val = MemoryMap.map_
            for x in args:
                val = val[x]
            return val
        except:
            return defval

    @staticmethod
    def hset(*args, overwrite=True):
        if not isinstance(args,list) :
            list_ = list(args)
        else :
            list_ = args

        value = list_.pop()
        last_key = list_.pop()

        map_tmp = MemoryMap.map_
        for l in list_:
            if l in map_tmp:
                map_tmp = map_tmp[l]
            else:
                map_tmp[l] = {}
                map_tmp = map_tmp[l]

        if last_key in map_tmp and not overwrite:
            raise Exception(f'Key already in use : {list_}')

        map_tmp[last_key] = value


MemoryMap.hset("1", "2", "3", "4", "bla_bla")
MemoryMap.hset("1", "2", "3", "4", "bla_bla_2")
MemoryMap.hset("1", "2", "3", "5", "bla_bla_3")

print(MemoryMap.map_)
print(MemoryMap.hget("1", "2", "3", "4"))
print(MemoryMap.hget("1", "2", "3", "5"))
print(MemoryMap.hget("1", "2", "8"))

# b=B()
# b.fun_b("a","1","2")
