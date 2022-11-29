import logging
import grpc
import json

import banking_pb2
import banking_pb2_grpc


class Customer:

    supported_write_operations = {"deposit", "withdraw"}

    def __init__(self, _id: int, events: list):
        # unique ID of the Customer
        self.id = _id

        # events from the input
        self.events = events

    def execute_events(self) -> None:
        """Processes the events from the list of events and submits the request to the Branch process"""

        customer_write_sets = []

        # will save the write sets from the last branch in the loop
        # (which should have all the write sets in the correct order)
        executed_branch_write_sets = None

        for event in self.events:

            # facilitate communication between customers and a branch process with matching ID
            with grpc.insecure_channel(f"localhost:5005{event['dest']}") as channel:

                # will be used to verify that the customer's write sets
                # were executed in the correct order by the branch
                if event["interface"] in self.supported_write_operations:
                    customer_write_sets.append(event["id"])

                stub = banking_pb2_grpc.BranchStub(channel)
                request = banking_pb2.BranchRequest(
                    event_id=event["id"],
                    interface=event["interface"],
                    money=event.get("money"),
                    type="customer",
                    id=self.id,
                    customer_id=self.id,
                )
                response = stub.MsgDelivery(request)
                executed_branch_write_sets = json.loads(response.write_sets)

                if response.status != 200:
                    logging.error(f"\nFailed to execute customer event: {event}")
                    raise ValueError(f"Event failed with status: {response.status}")

        logging.info("\nFinished executing customer events:")

        # confirm branch executed write sets match customer write sets
        if customer_write_sets == executed_branch_write_sets:
            logging.info("> Successfully executed write events in the correct order")

        else:
            logging.error(f"> Failed to successfully execute write events in the correct order:\n"
                          f"> customer_write_sets: {customer_write_sets}\n"
                          f"> executed_branch_write_sets: {executed_branch_write_sets}")
