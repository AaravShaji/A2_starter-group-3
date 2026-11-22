import json
from concurrent import futures

from flask import request
import grpc
import mysql.connector
import event_pb2 as pb
import event_pb2_grpc as rpc

CONFIG = json.load(open("config.json"))


def read(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor(dictionary=True) as cursor:
        cursor.execute(sql, args)
        return cursor.fetchall()


def write(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor() as cursor:
        cursor.execute(sql, args)
        db.commit()
        return cursor.lastrowid

def update(sql, args=()):
    with mysql.connector.connect(**CONFIG["db"]) as db, db.cursor() as cursor:
        cursor.execute(sql, args)
        db.commit()
        return cursor.rowcount

class EventService(rpc.EventServiceServicer):
    def GetEvent(self, request, context):
        found = read("""SELECT event_id, name, capacity, capacity - seats_taken AS available_seats
                        FROM events WHERE event_id = %s""", (request.event_id,))
        if not found:
            context.abort(grpc.StatusCode.NOT_FOUND, "Event not found")
        return pb.Event(**found[0])

    def ReleaseSeat(self, request, context):
        self.GetEvent(request, context)
        write("UPDATE events SET seats_taken = seats_taken - 1 WHERE event_id = %s", (request.event_id,))
        return pb.Empty()
    
    def ReserveSeat(self, request, context):
        self.GetEvent(request, context)
        taken = update ("""UPDATE events SET seats_taken = seats_taken + 1
                            WHERE event_id = %s AND seats_taken < capacity""", (request.event_id,))
        if taken == 0:
            context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Event is full")
        return pb.Empty()                        

    def ListEvents(self, request, context):
        found = read("""SELECT event_id, name, capacity, capacity - seats_taken AS available_seats
                        FROM events WHERE name LIKE %s
                        ORDER BY event_id ASC""", (f"%{request.name}%",))

        return pb.ListEventsResponse(
            events=[pb.Event(**row) for row in found]
        )
    
    def CreateEvent(self, request, context):
        # Remove spaces from the beginning and end of the name.
        name = request.name.strip()

        # Reject blank event names.
        if not name:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Event name cannot be blank"
            )

        # Reject names longer than the database allows.
        if len(name) > 150:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Event name cannot exceed 150 characters"
            )

        # Reject negative capacities.
        if request.capacity < 0:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "Capacity cannot be negative"
            )

        # Insert the event and retrieve its generated ID.
        event_id = write(
            "INSERT INTO events (name, capacity) VALUES (%s, %s)",
            (name, request.capacity)
        )

        # Return the newly created event.
        return self.GetEvent(
            pb.EventId(event_id=event_id),
            context
        )
    
    def UpdateEvent(self, request, context):
        # Check whether the event exists.
        self.GetEvent(
            pb.EventId(event_id=request.event_id),
            context
        )

        # Check which fields were provided in the request.
        has_name = request.HasField("name")
        has_capacity = request.HasField("capacity")

        # Reject requests that contain no fields to update.
        if not has_name and not has_capacity:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                "No fields provided for update"
            )

        # Validate the name if one was provided.
        if has_name:
            name = request.name.strip()

            # Reject blank names.
            if not name:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Event name cannot be blank"
                )

            # Reject names longer than 150 characters.
            if len(name) > 150:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Event name cannot exceed 150 characters"
                )

        # Validate capacity if it was provided.
        if has_capacity:
            if request.capacity < 0:
                context.abort(
                    grpc.StatusCode.INVALID_ARGUMENT,
                    "Capacity cannot be negative"
                )

        # Build the SQL update using the provided fields.
        changes = []
        args = []

        # Update the name only if it was provided.
        if has_name:
            changes.append("name = %s")
            args.append(name)

        # Update capacity only if it was provided.
        if has_capacity:
            changes.append("capacity = %s")
            args.append(request.capacity)

        # Add the event ID to the SQL arguments.
        args.append(request.event_id)

        # Build the SQL UPDATE statement.
        sql = (
            "UPDATE events SET " + ", ".join(changes) +
            " WHERE event_id = %s"
        )

        # Prevent capacity from dropping below booked seats.
        if has_capacity:
            sql += " AND seats_taken <= %s"
            args.append(request.capacity)

        # Execute the database update.
        changed = update(sql, tuple(args))

        # Check why the update did not change a row.
        if changed == 0:
            found = read(
                "SELECT seats_taken FROM events WHERE event_id = %s",
                (request.event_id,)
            )

            # Return NOT_FOUND if the event no longer exists.
            if not found:
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    "Event not found"
                )

            # Reject capacity below the number of booked seats.
            if has_capacity and request.capacity < found[0]["seats_taken"]:
                context.abort(
                    grpc.StatusCode.FAILED_PRECONDITION,
                    "Capacity cannot be below booked seats"
                )

        # Retrieve and return the updated event.
        return self.GetEvent(
            pb.EventId(event_id=request.event_id),
            context
        )
    
    def DeleteEvent(self, request, context):
        # Check whether the event exists.
        self.GetEvent(request, context)

        # Delete the event only if no seats are booked.
        deleted = update(
            """DELETE FROM events
               WHERE event_id = %s AND seats_taken = 0""",
            (request.event_id,)
        )

        # Reject deletion if the event has booked seats.
        if deleted == 0:
            context.abort(
                grpc.StatusCode.FAILED_PRECONDITION,
                "Cannot delete an event with bookings"
            )

        # Return an empty protobuf response.
        return pb.Empty()

    





server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
rpc.add_EventServiceServicer_to_server(EventService(), server)
server.add_insecure_port(CONFIG["listen"])
server.start()
print(f"Event service on {CONFIG['listen']}", flush=True)
server.wait_for_termination()
