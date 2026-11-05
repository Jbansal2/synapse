# Copyright 2021 Šimon Brandner <simon.bra.ag@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from copy import deepcopy
from typing import List
from unittest.mock import AsyncMock, patch

from twisted.test.proto_helpers import MemoryReactor

from synapse.api.constants import EduTypes, ReceiptTypes
from synapse.server import HomeServer
from synapse.types import JsonDict, ReadReceipt
from synapse.util import Clock

from tests import unittest


class ReceiptsTestCase(unittest.HomeserverTestCase):
    def prepare(self, reactor: MemoryReactor, clock: Clock, hs: HomeServer) -> None:
        self.event_source = hs.get_event_sources().sources.receipt
        self.receipts_handler = hs.get_receipts_handler()

    def test_filters_out_private_receipt(self) -> None:
        self._test_filters_private(
            [
                {
                    "content": {
                        "$1435641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ_PRIVATE: {
                                "@rikj:jki.re": {
                                    "ts": 1436451550453,
                                }
                            }
                        }
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
            [],
        )

    def test_filters_out_private_receipt_and_ignores_rest(self) -> None:
        self._test_filters_private(
            [
                {
                    "content": {
                        "$1dgdgrd5641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ_PRIVATE: {
                                "@rikj:jki.re": {
                                    "ts": 1436451550453,
                                },
                            },
                            ReceiptTypes.READ: {
                                "@user:jki.re": {
                                    "ts": 1436451550453,
                                },
                            },
                        },
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
            [
                {
                    "content": {
                        "$1dgdgrd5641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ: {
                                "@user:jki.re": {
                                    "ts": 1436451550453,
                                }
                            }
                        }
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
        )

    def test_filters_out_event_with_only_private_receipts_and_ignores_the_rest(
        self,
    ) -> None:
        self._test_filters_private(
            [
                {
                    "content": {
                        "$14356419edgd14394fHBLK:matrix.org": {
                            ReceiptTypes.READ_PRIVATE: {
                                "@rikj:jki.re": {
                                    "ts": 1436451550453,
                                },
                            }
                        },
                        "$1435641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ: {
                                "@user:jki.re": {
                                    "ts": 1436451550453,
                                }
                            }
                        },
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
            [
                {
                    "content": {
                        "$1435641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ: {
                                "@user:jki.re": {
                                    "ts": 1436451550453,
                                }
                            }
                        }
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
        )

    def test_handles_empty_event(self) -> None:
        self._test_filters_private(
            [
                {
                    "content": {
                        "$143564gdfg6114394fHBLK:matrix.org": {},
                        "$1435641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ: {
                                "@user:jki.re": {
                                    "ts": 1436451550453,
                                }
                            }
                        },
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
            [
                {
                    "content": {
                        "$1435641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ: {
                                "@user:jki.re": {
                                    "ts": 1436451550453,
                                }
                            }
                        },
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
        )

    def test_filters_out_receipt_event_with_only_private_receipt_and_ignores_rest(
        self,
    ) -> None:
        self._test_filters_private(
            [
                {
                    "content": {
                        "$14356419edgd14394fHBLK:matrix.org": {
                            ReceiptTypes.READ_PRIVATE: {
                                "@rikj:jki.re": {
                                    "ts": 1436451550453,
                                },
                            }
                        },
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                },
                {
                    "content": {
                        "$1435641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ: {
                                "@user:jki.re": {
                                    "ts": 1436451550453,
                                }
                            }
                        },
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                },
            ],
            [
                {
                    "content": {
                        "$1435641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ: {
                                "@user:jki.re": {
                                    "ts": 1436451550453,
                                }
                            }
                        }
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
        )

    def test_handles_string_data(self) -> None:
        """
        Tests that an invalid shape for read-receipts is handled.
        Context: https://github.com/matrix-org/synapse/issues/10603
        """

        self._test_filters_private(
            [
                {
                    "content": {
                        "$14356419edgd14394fHBLK:matrix.org": {
                            ReceiptTypes.READ: {
                                "@rikj:jki.re": "string",
                            }
                        },
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                },
            ],
            [
                {
                    "content": {
                        "$14356419edgd14394fHBLK:matrix.org": {
                            ReceiptTypes.READ: {
                                "@rikj:jki.re": "string",
                            }
                        },
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                },
            ],
        )

    def test_received_remote_receipt_ignores_malformed_entries(self) -> None:
        """
        A malformed `m.receipt` EDU from a remote server (e.g. a room's value,
        a receipt type's value, or a user's value being a string instead of an
        object) should not raise, and should not prevent other, well-formed
        receipts in the same EDU from being processed.

        Context: https://github.com/home-assistant/core/issues/153638 is an
        unrelated report, but https://github.com/matrix-org/synapse/issues/10376
        is the original report of this crash.
        """
        room_id = "!room:remote.example.org"
        good_user_id = "@good:remote.example.org"

        content: JsonDict = {
            # The whole room's value is a string, not an object.
            "!malformed-room-value:remote.example.org": "not a dict",
            room_id: {
                # The receipt type's value is a string, not an object.
                ReceiptTypes.READ: "not a dict",
                "a.receipt.type": {
                    good_user_id: {
                        "event_ids": ["$event:remote.example.org"],
                        "data": {"ts": 1436451550453},
                    },
                    # This user's value is a string, not an object.
                    "@malformed:remote.example.org": "not a dict",
                    # This user's event_ids list has non-string elements.
                    "@malformed-event-ids:remote.example.org": {
                        "event_ids": [1, None],
                    },
                },
            },
        }

        with patch.object(
            self.receipts_handler.event_auth_handler,
            "is_host_in_room",
            new=AsyncMock(return_value=True),
        ), patch.object(
            self.receipts_handler._storage_controllers.state,
            "get_current_hosts_in_room_or_partial_state_approximation",
            new=AsyncMock(return_value=["remote.example.org"]),
        ), patch.object(
            self.receipts_handler, "_handle_new_receipts", new=AsyncMock()
        ) as mock_handle_new_receipts:
            self.get_success(
                self.receipts_handler._received_remote_receipt(
                    "remote.example.org", content
                )
            )

        mock_handle_new_receipts.assert_called_once()
        (received_receipts,) = mock_handle_new_receipts.call_args.args
        self.assertEqual(
            received_receipts,
            [
                ReadReceipt(
                    room_id=room_id,
                    receipt_type="a.receipt.type",
                    user_id=good_user_id,
                    event_ids=["$event:remote.example.org"],
                    thread_id=None,
                    data={"ts": 1436451550453},
                )
            ],
        )

    def test_leaves_our_private_and_their_public(self) -> None:
        self._test_filters_private(
            [
                {
                    "content": {
                        "$1dgdgrd5641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ_PRIVATE: {
                                "@me:server.org": {
                                    "ts": 1436451550453,
                                },
                            },
                            ReceiptTypes.READ: {
                                "@rikj:jki.re": {
                                    "ts": 1436451550453,
                                },
                            },
                            "a.receipt.type": {
                                "@rikj:jki.re": {
                                    "ts": 1436451550453,
                                },
                            },
                        },
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
            [
                {
                    "content": {
                        "$1dgdgrd5641916114394fHBLK:matrix.org": {
                            ReceiptTypes.READ_PRIVATE: {
                                "@me:server.org": {
                                    "ts": 1436451550453,
                                },
                            },
                            ReceiptTypes.READ: {
                                "@rikj:jki.re": {
                                    "ts": 1436451550453,
                                },
                            },
                            "a.receipt.type": {
                                "@rikj:jki.re": {
                                    "ts": 1436451550453,
                                },
                            },
                        }
                    },
                    "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                    "type": EduTypes.RECEIPT,
                }
            ],
        )

    def test_we_do_not_mutate(self) -> None:
        """Ensure the input values are not modified."""
        events = [
            {
                "content": {
                    "$1435641916114394fHBLK:matrix.org": {
                        ReceiptTypes.READ_PRIVATE: {
                            "@rikj:jki.re": {
                                "ts": 1436451550453,
                            }
                        }
                    }
                },
                "room_id": "!jEsUZKDJdhlrceRyVU:example.org",
                "type": EduTypes.RECEIPT,
            }
        ]
        original_events = deepcopy(events)
        self._test_filters_private(events, [])
        # Since the events are fed in from a cache they should not be modified.
        self.assertEqual(events, original_events)

    def _test_filters_private(
        self, events: List[JsonDict], expected_output: List[JsonDict]
    ) -> None:
        """Tests that the _filter_out_private returns the expected output"""
        filtered_events = self.event_source.filter_out_private_receipts(
            events, "@me:server.org"
        )
        self.assertEqual(filtered_events, expected_output)
