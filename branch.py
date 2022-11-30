import json
import logging
from typing import Union, Any
from collections import defaultdict

import grpc
import banking_pb2
import banking_pb2_grpc


class Branch(banking_pb2_grpc.BranchServicer):

    supported_write_operations = {"deposit", "withdraw"}

    def __init__(self, _id: int, balance: int, branches: list):
        # unique ID of the Branch
        self.id = _id

        # replica of the Branch's balance
        self.balance = balance

        # the list of process IDs of the branches (excluding current one)
        self.branches = branches

        # keep track of write operations for a given client
        self.write_set = defaultdict(list)

        # keep track of all operations for a given client
        self.event_tracker = defaultdict(defaultdict)

    def collect_events(self, request: Any) -> None:
        """Helper method that validates and saves received events"""

        # collect write events (organized by customer id)
        if request.interface in self.supported_write_operations:
            if not self.write_set[request.customer_id]:
                self.write_set[request.customer_id].append(request.event_id)
            else:
                # validate event order
                last_event_received = self.write_set[request.customer_id][-1]
                if last_event_received >= request.event_id:
                    raise ValueError(f"Event {request.event_id} in incorrect order")
                else:
                    self.write_set[request.customer_id].append(request.event_id)

            logging.debug(f"\t> branch {self.id} adding to writeset event {request.event_id}")

        # collect all events (organized by customer id)
        self.event_tracker[request.customer_id][request.event_id] = {
            "interface": request.interface,
            "money": request.money,
            "event_id": request.event_id,
        }

    def read_writes(self, request: Any) -> None:
        """Helper method that validates that write sets and balances are in sync across all branches"""

        logging.debug("\t\tValidating that all writes have propagated to all branches:")

        # set that stores all branches balances
        # if all balances match, then this set will only have one item
        all_balances = {self.balance}

        # set that stores all branches write_sets
        # if all write_sets match, then this set will only have one item
        all_write_sets = {self.get_current_write_sets(request.customer_id)}

        for target_branch in self.branches:
            # creates a gRPC channel to a specific branch
            with grpc.insecure_channel(f"localhost:5005{target_branch}") as channel:
                stub = banking_pb2_grpc.BranchStub(channel)
                request = banking_pb2.BranchRequest(read_writes=True, customer_id=request.customer_id)
                response = stub.MsgDelivery(request)

                if response.status != 200:
                    logging.error(f"\nFailed to read balance from {target_branch}")
                    raise ValueError(f"Event failed with status: {response.status}")
                else:
                    all_balances.add(response.balance)
                    all_write_sets.add(response.write_sets)

        if len(all_balances) > 1:
            raise ValueError("Balances across branches don't match")
        elif len(all_write_sets) > 1:
            raise ValueError("Write sets across branches don't match")
        else:
            logging.debug("\t\t> Propagation successful!")
            logging.debug(f"\t******************************************************************")

    def get_current_write_sets(self, customer_id: int) -> str:
        """Get the branch's write sets for a given customer id"""
        return json.dumps(self.write_set[customer_id])

    def MsgDelivery(self, request: Any, context: Any) -> Any:
        """Processes the requests received from other processes and returns results to requested process."""
        try:
            if request.read_writes is True:
                # handle a read_writes request. sent by other branches to verify
                # that the write operation has been executed in the correct order
                return banking_pb2.BranchReply(
                    status=200,
                    balance=self.balance,
                    write_sets=self.get_current_write_sets(request.customer_id)
                )
            else:
                # handle deposit, withdraw, and query requests
                logging.debug(
                    f"\t> branch {self.id} received {request.interface} from "
                    f"{request.type} {request.id}"
                    f"{' for the amount of: $ ' + str(request.money) if request.interface != 'query' else ''}"
                )

                # store event information
                self.collect_events(request)

                if request.type == "branch":
                    if request.interface == "deposit":
                        self.deposit(request.money)
                    elif request.interface == "withdraw":
                        self.withdraw(request.money)
                    logging.debug(f"\t> branch {self.id} balance is ${self.balance}")

                if request.type == "customer":
                    if request.interface == "query":
                        logging.debug(f"\t> branch {self.id} balance is ${self.balance}")
                    else:
                        if request.interface == "deposit":
                            self.deposit(request.money)
                            self._propagate_to_branches(request, request.id)
                        elif request.interface == "withdraw":
                            self.withdraw(request.money)
                            self._propagate_to_branches(request, request.id)

                        # validate writes have been propagated
                        self.read_writes(request)

        except Exception as e:
            logging.error(f"\n{request.type.capitalize()} transaction failed with error: {e}")
            return banking_pb2.BranchReply(status=400)
        else:
            # event request has been completed
            return banking_pb2.BranchReply(
                status=200,
                write_sets=json.dumps(self.write_set[request.customer_id])
            )

    def _propagate_to_branches(self, request: Any, customer_id: int) -> None:
        """Helper that propagates deposits or withdrawals to other branches"""

        logging.debug(
            f"\t** propagating branch {self.id} {request.interface} "
            f"of ${request.money} to branches {self.branches}:"
        )

        for target_branch in self.branches:
            params = dict(
                id=self.id,
                interface=request.interface,
                money=request.money,
                type="branch",
                event_id=request.event_id,
                customer_id=customer_id
            )

            # create a gRPC channel to a specific branch
            with grpc.insecure_channel(f"localhost:5005{target_branch}") as channel:
                stub = banking_pb2_grpc.BranchStub(channel)
                request = banking_pb2.BranchRequest(**params)
                response = stub.MsgDelivery(request)

                if response.status != 200:
                    logging.error(f"\nFailed to execute branch request: {params}")
                    raise ValueError(f"Event failed with status: {response.status}")

        logging.debug(f"\t******************************************************************")

    def deposit(self, amount: Union[int, float]) -> None:
        """Initiate branch deposit"""
        self.balance += amount

    def withdraw(self, amount: Union[int, float]) -> None:
        """Initiate branch withdraw"""
        self.balance -= amount


class BranchDebugger:
    """Helper class for debugging branch processes"""

    def __init__(self, branches: list):
        self.branches = branches

    def log_balances(self, note: str) -> None:
        logging.info(f"\nBranch balances ({note}):")
        for b in self.branches:
            logging.info(f"\t- id: {b.id}, balance: {b.balance}")

    def list_branch_events(self) -> None:
        logging.info("\nSANITY CHECK:\nWrite sets per branch organized by customer id:\n"
                     "> ie: {branch_id: {customer_id: [write_event_1, ...]}}")
        logging.info(json.dumps([{f"branch_{b.id}": b.write_set} for b in self.branches], indent=4))

        logging.info("\nSANITY CHECK:\nAll events per branch organized by customer id:\n"
                     "> ie: {branch_id: {customer_id: [event_1, ...]}}")
        logging.info(json.dumps([{f"branch_{b.id}": b.event_tracker} for b in self.branches], indent=4))

    def validate_consistency_and_get_final_balance(self) -> None:
        # when all branch balances are in sync, the set should only have 1 object
        final_balance = {b.balance for b in self.branches}

        if len(final_balance) > 1:
            raise ValueError("Different balances are present across the different branches. Failed consistency check.")

        else:
            # retrieving customers present
            # (assuming all branches have all available customers id at this point)
            # so we will retrieve the ids present in the first branch
            output = [{"id": cus_id, "balance": final_balance.pop()} for cus_id in self.branches[0].write_set.keys()]
            logging.info(f"\nOutput:\n{json.dumps(output, indent=4)}")
