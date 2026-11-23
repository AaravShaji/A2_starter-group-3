import json
from concurrent import futures

import grpc
import mysql.connector
import event_pb2
import event_pb2_grpc
import registration_pb2 as pb
import registration_pb2_grpc as rpc

CONFIG = json.load(open("config.json"))

def read(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor(
        dictionary=True
    ) as cursor:
        cursor.execute(sql, args)
        return cursor.fetchall()

def write(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor() as cursor:
        cursor.execute(sql, args)
        db.commit()
        return cursor.lastrowid

def call(method, request, context):
    try:
        return method(request)
    except grpc.RpcError as error:
        context.abort(error.code(), error.details())

class RegistrationService(rpc.RegistrationServiceServicer):
    def __init__(self, events):
        self.events = events

    def CancelBooking(self, request, context):
        found = read(
            "SELECT event_id, state FROM bookings WHERE booking_id = %s",
            (request.booking_id,),
        )
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Booking not found")
        if found[0]["state"] == "confirmed":
            write(
                "UPDATE bookings SET state = 'cancelled' WHERE booking_id = %s",
                (request.booking_id,),
            )
            call(
                self.events.ReleaseSeat,
                event_pb2.EventId(event_id=found[0]["event_id"]),
                context,
            )
        return pb.Empty()

    # Task 3 implementation server side, Get implementation
    def GetParticipant(self, request, context):
        found = read(
            "SELECT participant_id, name FROM participants WHERE participant_id = %s",
            (request.participant_id,),
        )
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Participant not found")
        return pb.Participant(**found[0])

    # Task 3 implementation server side, Create participant
    def CreateParticipant(self, request, context):
        name = request.name.strip()
        if not name:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT, "Participant name cannot be blank"
            )
        participant_id = write("INSERT INTO participants (name) VALUES (%s)", (name,))
        return self.GetParticipant(
            pb.ParticipantId(participant_id=participant_id), context
        )

    # Task 4 implementation server side, Create booking for event
    def CreateBooking(self, request, context):
        self.GetParticipant(
            pb.ParticipantId(participant_id=request.participant_id), context
        )
        existing = read(
            """SELECT booking_id FROM bookings WHERE event_id = %s AND participant_id = %s AND state
                         = 'confirmed'""",
            (request.event_id, request.participant_id),
        )
        if existing:
            context.abort(
                grpc.StatusCode.ALREADY_EXISTS,
                "Participant is already registered for this event",
            )
        call(
            self.events.ReserveSeat,
            event_pb2.EventId(event_id=request.event_id),
            context,
        )
        try:
            booking_id = write(
                """INSERT INTO bookings (event_id, participant_id) VALUES (%s, %s)""",
                (request.event_id, request.participant_id),
            )
        except mysql.connector.Error:
            call(
                self.events.ReleaseSeat,
                event_pb2.EventId(event_id=request.event_id),
                context,
            )
            context.abort(grpc.StatusCode.INTERNAL, "Could not save booking")
        found = read(
                """SELECT b.booking_id, b.event_id, b.participant_id, p.name AS participant_name FROM bookings b
               JOIN participants p ON b.participant_id = p.participant_id WHERE b.booking_id = %s""",
                (booking_id,),
            )

        return pb.Booking(**found[0])

events = event_pb2_grpc.EventServiceStub(grpc.insecure_channel(CONFIG["event_service"]))
server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
rpc.add_RegistrationServiceServicer_to_server(RegistrationService(events), server)
server.add_insecure_port(CONFIG["listen"])
server.start()
print(f"Registration service on {CONFIG['listen']}", flush=True)
server.wait_for_termination()
