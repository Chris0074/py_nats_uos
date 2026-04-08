import sys
import pathlib
import pytest

# Add the project root to Python path to enable absolute imports
ROOT_PATH = pathlib.Path(__file__).resolve().parent.parent
if str(ROOT_PATH) not in sys.path:
    sys.path.insert(0, str(ROOT_PATH))
SRC_PATH = ROOT_PATH.joinpath("src")
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from building import Building, Raffstore


class DummyAccessProvider:
    def __init__(self):
        self.connected = False
        self.subscriptions = []
        self.writes = []

    async def connect(self):
        self.connected = True

    async def get_definition(self):
        return ({
            "in_up": 1,
            "in_down": 2,
            "out_up": 3,
            "out_down": 4,
        }, {
            1: None,
            2: None,
            3: None,
            4: None,
        })

    async def subscribe_change(self, key, callback):
        self.subscriptions.append((key, callback))

    async def write_value(self, key, value):
        self.writes.append((key, value))


@pytest.fixture(autouse=True)
def reset_building_state():
    Building._access_provider = None
    Building._variables_names = None
    Building._variables_ids = None
    Building._setup_finished = False
    yield
    Building._access_provider = None
    Building._variables_names = None
    Building._variables_ids = None
    Building._setup_finished = False


def test_raffstore_instances_are_independent():
    provider = DummyAccessProvider()
    Building._access_provider = provider

    first = Raffstore(in_up="input_a_up", in_down="input_a_down", out_up="output_a_up", out_down="output_a_down", run_time=5)
    second = Raffstore(in_up="input_b_up", in_down="input_b_down", out_up="output_b_up", out_down="output_b_down", run_time=6)

    assert first is not second
    assert first.in_up == "input_a_up"
    assert second.in_up == "input_b_up"
    assert first.out_down == "output_a_down"
    assert second.out_down == "output_b_down"

    assert first._delay_timer is not second._delay_timer
    assert first._short_run_timer is not second._short_run_timer
    assert first._long_run_timer is not second._long_run_timer

    # unique active flags per instance
    first._up_active = True
    second._down_active = True
    assert first._up_active is True
    assert first._down_active is False
    assert second._up_active is False
    assert second._down_active is True


def test_shared_access_provider_can_be_reused():
    first_provider = DummyAccessProvider()
    Building._access_provider = first_provider

    first = Raffstore(in_up="input_c_up", in_down="input_c_down", out_up="output_c_up", out_down="output_c_down", run_time=7)
    second = Raffstore(in_up="input_d_up", in_down="input_d_down", out_up="output_d_up", out_down="output_d_down", run_time=8)

    assert first.access is first_provider
    assert second.access is first_provider
    assert Building._access_provider is first_provider

    # Both instances should share the provider, but not object state.
    assert first is not second
    assert first.in_up != second.in_up
