import threading
import grpc
import traceback
from concurrent import futures
import mathdb_pb2_grpc
import mathdb_pb2


class MathCache:
    def __init__(self):
        self.cache = {}
        self.cache_size = 10
        self.key_dict = {} 
        self.evict_order = [] 
        self.lock = threading.Lock()
        
    def Set(self,key, value):
        with self.lock:
            self.key_dict[key] = value
            self.cache.clear()
            self.evict_order.clear()

    def Get(self, key):
        return self.key_dict[key]
    
    def Add(self, key_a, key_b):
        operation_key = ("add", f'"{key_a}"', f'"{key_b}"')
        with self.lock:
            if operation_key in self.cache:
                cache_hit =  True 
                self.evict_order.remove(operation_key)
                self.evict_order.append(operation_key)
            
                try:
                    result = self.Get(key_a)+self.Get(key_b)
                except KeyError:
                    raise KeyError
            else:            
                cache_hit =  False
                self.evict_order.append(operation_key)
                self.cache[operation_key] = cache_hit
                if len(self.cache) > self.cache_size:
                    victim = self.evict_order.pop(0)  # pop from the front
                    self.cache.pop(victim)
                try:
                    result = self.Get(key_a)+self.Get(key_b)
                except KeyError:
                    raise KeyError
            return result, cache_hit
       
    def Sub(self, key_a, key_b):
        operation_key = ("sub", f'"{key_a}"', f'"{key_b}"')
        with self.lock:
            if operation_key in self.cache:
                cache_hit =  True 
                self.evict_order.remove(operation_key)
                self.evict_order.append(operation_key)
            
                try:
                    result = self.Get(key_a)-self.Get(key_b)
                except KeyError:
                    raise KeyError
            else:            
                cache_hit =  False
                self.evict_order.append(operation_key)
                self.cache[operation_key] = cache_hit
                if len(self.cache) > self.cache_size:
                    victim = self.evict_order.pop(0)  # pop from the front
                    self.cache.pop(victim)
                try:
                    result = self.Get(key_a)-self.Get(key_b)
                except KeyError:
                    raise KeyError
            return result, cache_hit            

    def Mult(self, key_a, key_b):
        operation_key = ("mult", f'"{key_a}"', f'"{key_b}"')
        with self.lock:
            if operation_key in self.cache:
                cache_hit =  True 
                self.evict_order.remove(operation_key)
                self.evict_order.append(operation_key)
                
                try:
                    result = self.Get(key_a)*self.Get(key_b)
                except KeyError:
                    raise KeyError
            else:            
                cache_hit =  False
                self.evict_order.append(operation_key)
                self.cache[operation_key] = cache_hit
                if len(self.cache) > self.cache_size:
                    victim = self.evict_order.pop(0)  # pop from the front
                    self.cache.pop(victim)
                try:
                    result = self.Get(key_a)*self.Get(key_b)
                except KeyError:
                    raise KeyError
            return result, cache_hit   

    def Div(self, key_a, key_b):
        operation_key = ("div", f'"{key_a}"', f'"{key_b}"')
        with self.lock:
            if operation_key in self.cache:
                cache_hit =  True 
                self.evict_order.remove(operation_key)
                self.evict_order.append(operation_key)
                
                try:
                    result = self.Get(key_a)/self.Get(key_b)
                except KeyError:
                    raise KeyError
            else:            
                cache_hit =  False
                self.evict_order.append(operation_key)
                self.cache[operation_key] = cache_hit
                if len(self.cache) > self.cache_size:
                    victim = self.evict_order.pop(0)  # pop from the front
                    self.cache.pop(victim)
                try:
                    result = self.Get(key_a)/self.Get(key_b)
                except KeyError:
                    raise KeyError
            return result, cache_hit   

class MathDb(mathdb_pb2_grpc.MathDbServicer):
    def __init__(self):
        self.db = MathCache()
    
    def Set(self, request, context):
        try:
            err = ""
            self.db.Set(request.key, request.value) 
        except Exception:
            err = traceback.format_exc()
        return mathdb_pb2.SetResponse(error = err)
        
    def Get(self, request, context):
        res = None
        try:
            err = ""
            res =  self.db.Get(request.key)
        except Exception:
             err = traceback.format_exc()
        return mathdb_pb2.GetResponse(value = res, error = err)

    def Add(self, request, context):
        res = None
        hit = False
        try:
            err = ""
            res, hit =  self.db.Add(request.key_a, request.key_b)
        except Exception:
            err = traceback.format_exc()
        return mathdb_pb2.BinaryOpResponse(value = res, cache_hit = hit, error = err)

    def Sub(self, request, context):
        res = None
        hit = False
        try:
            err = ""
            res, hit =  self.db.Sub(request.key_a, request.key_b)
        except Exception:
            err = traceback.format_exc()
        return mathdb_pb2.BinaryOpResponse(value = res, cache_hit = hit, error = err)

    def Mult(self, request, context):
        res = None
        hit = False
        try:
            err = ""
            res, hit = self.db.Mult(request.key_a, request.key_b)
        except Exception:
            err = traceback.format_exc()
        return mathdb_pb2.BinaryOpResponse(value = res, cache_hit = hit, error = err)

    def Div(self, request, context):
        res = None
        hit = False
        try:
            err = ""
            res, hit =  self.db.Div(request.key_a, request.key_b)
        except Exception:
            err = traceback.format_exc()
        return mathdb_pb2.BinaryOpResponse(value = res, cache_hit = hit, error = err)



if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
    mathdb_pb2_grpc.add_MathDbServicer_to_server(MathDb(), server)
    server.add_insecure_port("[::]:5440", )
    server.start()
    server.wait_for_termination()
