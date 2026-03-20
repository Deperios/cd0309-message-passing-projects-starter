import grpc
from concurrent import futures
import location_pb2
import location_pb2_grpc
import psycopg2
import os

class LocationService(location_pb2_grpc.LocationServiceServicer):

    def GetLocations(self, request, context):
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_NAME"),
            user=os.environ.get("DB_USERNAME"),
            password=os.environ.get("DB_PASSWORD")
            port=5432
        )
        cursor = conn.cursor()

        query = """
        SELECT person_id, ST_X(coordinate), ST_Y(coordinate), creation_time
        FROM location
        WHERE person_id = %s
        AND creation_time BETWEEN %s AND %s;
        """

        cursor.execute(query, (
            request.person_id,
            request.start_date,
            request.end_date
        ))

        rows = cursor.fetchall()

        locations = []
        for row in rows:
            locations.append(location_pb2.Location(
                person_id=row[0],
                longitude=row[1],
                latitude=row[2],
                creation_time=str(row[3])
            ))

        conn.close()

        return location_pb2.LocationResponse(locations=locations)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    location_pb2_grpc.add_LocationServiceServicer_to_server(LocationService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()