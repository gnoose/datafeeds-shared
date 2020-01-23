import logging
from typing import Callable, Any, List, Dict, Optional

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait

from datafeeds.common.webdriver.drivers.base import BaseDriver

logger = None
log = logging.getLogger(__name__)


class NoInitialStateException(Exception):
    """Inidicates that an initial state was not set for a PageStateMachine"""

    pass


class MissingStateException(Exception):
    """Raised when a non-existent state is accessed in PageStateMachine"""

    def __init__(self, name: str, message: str = ""):
        self.name = name

        if not message:
            message = "A state named '{}' does not exist.".format(name)
        super().__init__(message)


class TransitionTimeoutException(Exception):
    """This exception is used by the PageStateMachine to indicate when a transition fails to occur."""

    def __init__(self, source: str, dest: List[str], message: str = ""):
        self.source = source
        self.dest = dest

        if not message:
            if len(dest) == 1:
                message = "Failed to transition from state '{}' to state '{}'".format(
                    source, dest[0]
                )
            else:
                message = "Failed to transition from state '{}' to one of {}".format(
                    source, sorted(dest)
                )

        super().__init__(message)


class PageState:
    """A simple Page Object class for Selenium scrapers

    This may be useful for modeling pages for a Selenium scraper. It is not currently used everywhere.
    """

    def __init__(self, driver: BaseDriver):
        self.driver = driver

    def get_ready_condition(self) -> Callable[[BaseDriver], Any]:
        """This should be implemented by subclasses. Returns a predicate indicating when this page is "ready"

        This can return for example any of the Selenium ExpectedCondition predicates, e.g.:
        def get_ready_condition(self):
            return EC.presence_of_element_located((By.ID, "some_id"))
        ...or any other function that accepts a WebDriver object
        """
        raise NotImplementedError()

    def is_ready(self) -> bool:
        """Determines whether the page is ready, using get_ready_condition"""
        ready_condition = self.get_ready_condition()
        if ready_condition:
            try:
                return ready_condition(self.driver)
            except:  # noqa E722
                # By default, exceptions are interpreted as "not ready"
                return False
        return True


class StateNode:
    """Represents a node in a PageState machine. It is mostly intended for internal use by that class.

    Args:
        page: A page object associated with this state node (can be None)
        action: A callable function associated with this state node (can be None)
        transitions: A list of state names one can transition to from this state.
        wait_time: How long to wait when attempting to perform a transition from this node.
    """

    def __init__(
        self, page: PageState, action: Callable, transitions: List[str], wait_time: int
    ):
        self.page = page
        self.action = action
        self.transitions = transitions
        self.wait_time = wait_time


class PageStateMachine:
    """A state machine for modeling simple web pages, for Selenium scrapers.

    A state consists of:
        1) A unique name
        2) An optional PageState object. The "get_ready_condition" function on the page is used to determine whether
           the state is ready to enter, if present. If no page is specified, the state is always ready to be entered.
        3) An optional action, which is executed upon entering the state, if present.
        4) A set of transition states. After executing the action associated with a state (if any), we try to
           transition to another state. The first "ready" state in the transition list is chosen.
        5) A wait time. This is how long the driver will wait for a subsequent state to be ready, while transitioning.
    """

    def __init__(self, driver: BaseDriver):
        self.driver = driver
        self.state_machine: Dict[str, StateNode] = dict()
        self.initial_state: Optional[str] = None
        self.on_enter_state_fn: Optional[Callable] = None

    def set_initial_state(self, name: str):
        if name not in self.state_machine:
            raise MissingStateException(name=name)
        self.initial_state = name

    def on_enter_state(self, cb: Callable):
        self.on_enter_state_fn = cb

    def add_state(
        self,
        name: str,
        page: PageState = None,
        action: Callable = None,
        transitions: List[str] = None,
        wait_time: int = 15,
    ):
        if not transitions:
            transitions = []
        self.state_machine[name] = StateNode(
            page=page, action=action, transitions=transitions, wait_time=wait_time
        )

    def run(self) -> str:
        """Run the state machine from the initial state.

        Starting, from the initial state, we:
            1) Execute the action associated with the state
            2) Wait for one of the transition states to be "ready", and transition to it. If multiple states are
               "ready", we transition to the first such state encountered in the transition set.

        This continues until we reach a state with no defined transitions. If an exception occurs during any of this,
        it is transparently raised to the caller.
        """

        if not self.initial_state:
            raise NoInitialStateException("No initial state set, aborting.")

        cur_state_name = self.initial_state

        done = False
        while not done:
            cur_state = self.state_machine.get(cur_state_name)
            if not cur_state:
                raise MissingStateException(cur_state_name)

            log.info("Entering state: {}".format(cur_state_name))
            if self.on_enter_state_fn:
                self.on_enter_state_fn(cur_state_name)

            if cur_state.action:
                log.info("Performing action in state: {}".format(cur_state_name))
                cur_state.action(cur_state.page)

            if cur_state.transitions:
                log.info(
                    "Attempting transition: src={}, dest={}".format(
                        cur_state_name, cur_state.transitions
                    )
                )
                wait = WebDriverWait(self.driver, cur_state.wait_time)
                try:
                    cur_state_name = wait.until(
                        transition_is_ready(self.state_machine, cur_state_name)
                    )
                except TimeoutException as e:
                    log.info(
                        "Transition timed out: src={}, dest={}".format(
                            cur_state_name, cur_state.transitions
                        )
                    )
                    raise TransitionTimeoutException(
                        source=cur_state_name, dest=cur_state.transitions
                    ) from e
            else:
                done = True

        return cur_state_name

    def validate(self):
        """Validate the state machine configuration.

        In particular, ensure that all specified transitions are legal.
        """
        for name, state in self.state_machine.items():
            for transition in state.transitions:
                if transition not in self.state_machine:
                    raise MissingStateException(
                        name=transition,
                        message="State '{0}' transitions to unknown state '{1}'.".format(
                            name, transition
                        ),
                    )


class transition_is_ready:
    """This wait predicate is used by the PageStateMachine to wait for a transition state to be ready."""

    def __init__(self, state_machine: dict, cur_state_name: str):
        self.state_machine = state_machine
        self.cur_state_name = cur_state_name

    def __call__(self, driver):
        cur_state = self.state_machine[self.cur_state_name]

        # Note that we find the first transition in the list that is ready. It's possible that multiple such states will
        # be ready, but we just choose the first one for now. This might need to be revisited later.
        for dest_state_name in cur_state.transitions:
            dest_state = self.state_machine.get(dest_state_name)
            if not dest_state:
                raise MissingStateException(dest_state_name)

            if dest_state.page:
                if dest_state.page.is_ready():
                    return dest_state_name
            else:
                return dest_state_name

        return None


class page_is_ready:
    def __init__(self, state: PageState):
        self.state = state

    def __call__(self, driver):
        return self.state.is_ready()
