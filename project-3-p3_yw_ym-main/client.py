import sys
import csv
import grpc
import threading
import mathdb_pb2, mathdb_pb2_grpc

total_hits = 0
total = 0
lock = threading.Lock()
def process_file(address, file_path):
    global total
    global total_hits
    channel = grpc.insecure_channel(address)
    stub = mathdb_pb2_grpc.MathDbStub(channel)
    
    with open(file_path, 'r') as file:
        thread_hit = 0
        thread_total = 0
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            #thread_total += 1
            operation, *args = row
            if operation.lower() == "set":
                key, value = args
                response = stub.Set(mathdb_pb2.SetRequest(key=key, value=float(value)))
                #print(f'Set {key}={value}: {response.error}')
            elif operation.lower() == "get":
                key = args[0]
                response = stub.Set(mathdb_pb2.GetRequest(key=key))
                #print(f'Get {key}={response.value}: {response.error}')
            else:
                key_a, key_b = args
                request =mathdb_pb2.BinaryOpRequest(key_a=key_a, key_b=key_b)
                if operation.lower() == 'add':
                    response = stub.Add(request)
                elif operation.lower() == 'sub':
                    response = stub.Sub(request)
                elif operation.lower() == 'mult':
                    response = stub.Mult(request)
                elif operation.lower() == 'div':
                    response = stub.Div(request)
                with lock:
                    thread_total += 1
                    if response.cache_hit:
                        thread_hit += 1
                    print(f'{operation} {key_a} {key_b}: value={response.value}, cache_hit={response.cache_hit}, hit_rate={thread_hit/thread_total},error={response.error}')
                #print(thread_hit/thread_total)
        total += thread_total
        total_hits += thread_hit


if len(sys.argv) < 2:
    print("Usage: python3 client.py <PORT> <THREAD1-WORK.csv> ...")
    sys.exit(1)               

port = sys.argv[1]        
csv_files = sys.argv[2:]
address = f"localhost:{port}"

threads = []
for csv_file in csv_files:
    t = threading.Thread(target=process_file, args=(address, csv_file))
    threads.append(t)
    t.start()
    
for t in threads:
    t.join()
    
print(f'{float(total_hits/total)}')
