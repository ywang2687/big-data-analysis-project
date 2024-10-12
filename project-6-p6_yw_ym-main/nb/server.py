from concurrent import futures
import grpc
from cassandra.cluster import Cluster, NoHostAvailable
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
from cassandra import Unavailable
import station_pb2
import station_pb2_grpc
from datetime import datetime


# This is from Jacob Nelson (PM)
class StationRecord: 
    def __init__(self, tmin, tmax):
        self.tmin = tmin
        self.tmax = tmax

class StationService(station_pb2_grpc.StationServicer):
    def __init__(self):
        self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])  
        self.session = self.cluster.connect('weather')
        self.insert_statement = self.session.prepare(
            "INSERT INTO stations (id, date, record) VALUES (?, ?, ?)"
        )
        self.insert_statement.consistency_level = ConsistencyLevel.ONE
        self.max_statement = self.session.prepare(
        "SELECT MAX(record.tmax) AS tmax FROM stations WHERE id=?"
        )
        self.max_statement.consistency_level = ConsistencyLevel.THREE
      
    def RecordTemps(self, request, context):
        station = request.station
        date = request.date
        record = StationRecord(request.tmin, request.tmax)
        try:
            self.cluster.register_user_type('weather', 'station_record', StationRecord)
            self.session.execute(
                self.insert_statement, (station, date, record)
            )
            return station_pb2.RecordTempsReply(error="")
        except Unavailable as e:
            error_message = f'need {e.required_replicas} replicas, but only have {e.alive_replicas}'
            return station_pb2.RecordTempsReply(error=error_message)
        except NoHostAvailable as e:
            for host, exception in e.errors.items():
                if isinstance(exception, Unavailable):
                    error_message = f'need {exception.required_replicas} replicas, but only have {exception.alive_replicas}'
                    return station_pb2.RecordTempsReply(error=error_message)
            return station_pb2.RecordTempsReply(error="No hosts available")
        except Exception as e:
            return station_pb2.RecordTempsReply(error=str(e))

    def StationMax(self, request, context):
       try:
            result = self.session.execute(
                self.max_statement, [request.station]
            )
            tmax = result[0].tmax if result else None
            return station_pb2.StationMaxReply(tmax=tmax, error="")
           
       except Unavailable as e:
            error_message = f'need {e.required_replicas} replicas, but only have {e.alive_replicas}'
            return station_pb2.StationMaxReply(error=error_message)
       except NoHostAvailable as e:
            for host, exception in e.errors.items():
                if isinstance(exception, Unavailable):
                    error_message = f'need {exception.required_replicas} replicas, but only have {exception.alive_replicas}'
                    return station_pb2.StationMaxReply(error=error_message)
            return station_pb2.StationMaxReply(error="No hosts available")
           
       except Exception as e:
            return station_pb2.StationMaxReply(error=str(e))




if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    station_pb2_grpc.add_StationServicer_to_server(StationService(), server)
    server.add_insecure_port("[::]:5440" )
    print("1")
    server.start()
    print("2")
    server.wait_for_termination()



