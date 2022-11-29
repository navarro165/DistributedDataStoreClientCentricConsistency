input_test_1 = [
    {
        "id": 1,
        "type": "customer",
        "events": [
            {"interface": "deposit", "money": 400, "id": 1, "dest": 1},
            {"interface": "withdraw", "money": 400, "id": 2, "dest": 2},
            {"interface": "query", "id": 3, "dest": 2},
        ],
    },
    {"id": 1, "type": "branch", "balance": 0},
    {"id": 2, "type": "branch", "balance": 0},
]

expected_output_1 = [{"id": 1, "balance": 0}]

input_test_2 = [
    {
        "id": 1,
        "type": "customer",
        "events": [
            {"interface": "deposit", "money": 400, "id": 1, "dest": 1},
            {"interface": "query", "id": 2, "dest": 2},
        ],
    },
    {"id": 1, "type": "branch", "balance": 0},
    {"id": 2, "type": "branch", "balance": 0},
]

expected_output_2 = [{"id": 1, "balance": 400}]
