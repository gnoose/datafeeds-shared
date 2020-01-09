from enum import Enum
import time
from typing import NewType

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import Sequence

from datafeeds import db

Base = declarative_base()


SEQUENCE_ID = "webapps_platform_oid_adapter_seq"


class OidGenerator:
    """The OidGenerator is a singleton (all state is stored in static members)."""

    _epoch = 1350654629.354
    node_id = None

    _counter = 0
    _clock = None

    #
    # In the platform system every server is assigned a Node ID between 0 and 4095 (inclusive). Any threads running
    # on the node uses a synchronized method to generate OIDs, and the OID is derived from the Node ID and the clock.
    # It's implicitly assumed that all the servers have (relatively) synchronized clocks.
    #
    # Node IDs are assigned via AWS tags on EC2 instances, which are then passed as configuration to platform
    # server processes by ansible. The problem with all of this is that webapps services are managed by docker
    # such that it's awkward to pass distinct identifiers to every container. A further complication is that
    # Webapps uses processes instead of threads to achieve concurrency, so each process actually
    # needs its own Node ID for the OID generation procedure to work as designed.
    #
    # The solution I settled on is to reserve a block of 1024 IDs (from 3072-4094) for webapps to use, and introduce
    # an increasing sequence in postgres that python processes can use to dynamically assign themselves a Node ID.
    # As long as we have fewer than 1024 total processes, assigning the node id as 3072 + (nextval(seq) % 1024))
    # ensures that every python process has a Node ID.
    #
    # cf. Platform repo: plugins/src/main/java/com/gridium/plugin/oid/permanent/PermanentGenerator.java
    #
    @classmethod
    def _determine_node_id(cls):
        with db.engine.connect() as conn:
            seq = Sequence(SEQUENCE_ID)
            next_id = conn.execute(seq)

        return (next_id % 1024) + 3072

    @classmethod
    def _check_clock(cls):
        while True:
            c = int(time.time() - OidGenerator._epoch)
            if c == OidGenerator._clock or (c & 0xFFFFFF8000000000 != 0):
                time.sleep(1)
            else:
                return c

    @classmethod
    def allocate(cls):
        """Synthesize a platform style OID."""
        if OidGenerator.node_id is None:
            OidGenerator.node_id = OidGenerator._determine_node_id()
            OidGenerator._counter = 0
            OidGenerator._clock = 0
            OidGenerator._clock = OidGenerator._check_clock()

        OidGenerator._counter += 1
        if OidGenerator._counter > 4095:
            OidGenerator._counter = 0
            OidGenerator._clock = OidGenerator._check_clock()

        # A Platform OID looks like this in binary:
        # - 52 bits in total.
        # - Top 12 Bits: Node ID
        # - Middle 27 Bits: Clock State
        # - Last 13 Bits: Counter State

        mask = 0x000FFFFFFFFFFFFF
        result = mask & (
            (OidGenerator.node_id << 52)
            | (OidGenerator._clock << 13)
            | OidGenerator._counter
        )
        return result


class ModelMixin:
    @classmethod
    def get_new_oid(cls):
        """Create a platform style object ID. Use this only for tables that don't have a sequence oid."""
        return OidGenerator.allocate()

    def jsonapi_attributes(self):
        attr = {}
        for col in self.__mapper__.columns:
            if col.name in ["oid", "id"]:
                continue

            val = getattr(self, col.name)

            # Extract value if is an enum
            # Manually extract value if it is a Python enum
            if issubclass(val.__class__, Enum):
                val = val.value

            # Ember wants dasherized property names
            attr[col.name.replace("_", "-")] = val

        return attr


Oid = NewType("Oid", int)
