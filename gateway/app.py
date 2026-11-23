import json

import grpc
from flask import Flask, abort, jsonify, request
from werkzeug.exceptions import HTTPException
import event_pb2
import event_pb2_grpc
import registration_pb2
import registration_pb2_grpc

CONFIG = json.load(open("config.json"))
STATUS = {
    grpc.StatusCode.INVALID_ARGUMENT: 400,
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.ALREADY_EXISTS: 409,
    grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
    grpc.StatusCode.FAILED_PRECONDITION: 409,
    grpc.StatusCode.UNIMPLEMENTED: 501,
    grpc.StatusCode.UNAVAILABLE: 503,
}

app = Flask(__name__)
events = event_pb2_grpc.EventServiceStub(grpc.insecure_channel(CONFIG["event_service"]))
registrations = registration_pb2_grpc.RegistrationServiceStub(
    grpc.insecure_channel(CONFIG["registration_service"])
)


def fields(message):
    return {
        field.name: getattr(message, field.name) for field in message.DESCRIPTOR.fields
    }


@app.errorhandler(grpc.RpcError)
def rpc_error(error):
    return jsonify(error=error.details()), STATUS.get(error.code(), 500)


@app.errorhandler(HTTPException)
def http_error(error):
    return jsonify(error=error.description), error.code


@app.before_request
def require_staff():

    # Only the add_event, edit_event, and delete_event endpoints require authentication.
    protected = ("add_event", "edit_event", "delete_event")

    # Checking that the requested endpoint requires authentication.
    if request.endpoint in protected:

        # Retrieve the token from the Authorization header.
        token = request.headers.get("Authorization")

        # Retrieve the expected staff token from the configuration.
        expected = "Bearer " + CONFIG["staff_token"]

        # Reject requests with missing or incorrect tokens.
        if token != expected:
            abort(401, "Unauthorized")


@app.get("/api/v1/events")
def list_events():
    name = request.args.get("name", "")
    response = events.ListEvents(event_pb2.ListEventsRequest(name=name))
    eventList = [fields(event) for event in response.events]
    return jsonify(eventList), 200


@app.get("/api/v1/events/<int:event_id>")
def get_event(event_id):
    return jsonify(fields(events.GetEvent(event_pb2.EventId(event_id=event_id))))


@app.post("/api/v1/events")
def add_event():
    body = request.get_json(silent=True)

    # Reject requests that do not contain a JSON object.
    if not isinstance(body, dict):
        abort(400, "Request body must be a valid JSON object")

    # Checking that the required fields are present.
    if "name" not in body or "capacity" not in body:
        abort(400, "Name and capacity are required")

    # Checking for supported fields
    if set(body) != {"name", "capacity"}:
        abort(400, "Only name and capacity are allowed")

    # Checking that the name is a string.
    if not isinstance(body["name"], str):
        abort(400, "Name must be a string")

    # Checking that capacity is an integer.
    if type(body["capacity"]) is not int:
        abort(400, "Capacity must be an integer")

    # Making sure capacity is not negative.
    if body["capacity"] < 0:
        abort(400, "Capacity cannot be negative")

    # Rejecting capacities larger than the protobuf int32 limit.
    if body["capacity"] > 2147483647:
        abort(400, "Capacity is too large")

    # Creating a protobuf request containing the event information.
    event_request = event_pb2.CreateEventRequest(
        name=body["name"], capacity=body["capacity"]
    )

    event = events.CreateEvent(event_request)

    return jsonify(fields(event)), 201


@app.patch("/api/v1/events/<int:event_id>")
def edit_event(event_id):

    body = request.get_json(silent=True)

    # Making sure the request body is a JSON object
    if not isinstance(body, dict):
        abort(400, "Request body must be a valid JSON object")

    # Making sure that the request body is not empty
    if not body:
        abort(400, "At least one field is required")

    # Rejecting fields other than name and capacity
    if not set(body).issubset({"name", "capacity"}):
        abort(400, "Only name and capacity can be updated")

    # Validating the name when provided
    if "name" in body:
        if not isinstance(body["name"], str):
            abort(400, "Name must be a string")

    # Validating the capacity when provided
    if "capacity" in body:
        if type(body["capacity"]) is not int:
            abort(400, "Capacity must be an integer")

        # Rejecting negative capacities.
        if body["capacity"] < 0:
            abort(400, "Capacity cannot be negative")

        # Rejecting capacities outside the protobuf int32 range.
        if body["capacity"] > 2147483647:
            abort(400, "Capacity is too large")

    # Creating a protobuf request containing the event ID.
    event_request = event_pb2.UpdateEventRequest(event_id=event_id)

    # Including the name only if it was provided.
    if "name" in body:
        event_request.name = body["name"]

    # Including the capacity only if it was provided.
    if "capacity" in body:
        event_request.capacity = body["capacity"]

    # Call the UpdateEvent method in the Event Service.
    event = events.UpdateEvent(event_request)

    # Return the updated event with HTTP status 200.
    return jsonify(fields(event)), 200


@app.delete("/api/v1/events/<int:event_id>")
def delete_event(event_id):
    # Creating a protobuf request containing the event ID.
    event_request = event_pb2.EventId(event_id=event_id)

    # Calling the DeleteEvent method in the Event Service
    events.DeleteEvent(event_request)

    return "", 204


@app.post("/api/v1/participants")
def add_participant():
    body = request.get_json(silent=True)
    if not isinstance(body, dict) or not isinstance(body.get("name"), str):
        abort(400, "Request body must be JSON with a string 'name'")
    participant = registrations.CreateParticipant(
        registration_pb2.CreateParticipantRequest(name=body["name"])
    )
    return jsonify(fields(participant)), 201


@app.get("/api/v1/participants/<int:participant_id>")
def get_participant(participant_id):
    return jsonify(
        fields(
            registrations.GetParticipant(
                registration_pb2.ParticipantId(participant_id=participant_id)
            )
        )
    )


@app.get("/api/v1/events/<int:event_id>/registrations")
def list_bookings(event_id):
    booking_request = registration_pb2.ListBookingsRequest(event_id=event_id)
    response = registrations.ListBookings(booking_request)
    booking_list = [fields(booking) for booking in response.bookings]
    return jsonify(booking_list), 200


@app.post("/api/v1/registrations")
def book():
    abort(501, "TODO task 4: implement POST /api/v1/registrations")


@app.delete("/api/v1/registrations/<int:booking_id>")
def cancel(booking_id):
    registrations.CancelBooking(registration_pb2.BookingId(booking_id=booking_id))
    return "", 204


app.run(host=CONFIG["host"], port=CONFIG["port"])
