# Selenium "PageState" Driver

This directory contains a simple library for expressing a Selenium Webdriver scraper
as a finite state machine.

## Tutorial

### PageState objects
Various browser states can be modeled by implementing subclasses of `PageState`
(defined in [pagestate.py](./pagestate.py)). For example, suppose your 
scraping task requires logging in to a site, then pulling data from a 
landing page. In this scenario, you might define two `PageState` classes, 
`LoginPage` and `LandingPage`. Each `PageState` implementation defines the 
set of conditions under which the state is "active", via overriding the
`get_ready_condition` function. Generally these conditions are expressed 
using the `ExpectedCondition` library from Selenium to evaluate the DOM state, 
but other predicates are possible as well. Here is a simple example of a
hypothetical `PageState` implementation for a login page.

```python
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from datafeeds.common.util.selenium import ec_and
from datafeeds.common.util.pagestate.pagestate import PageState

class LoginPage(PageState):
    UsernameInputLocator = (By.XPATH, "//input[@id='userName']")
    PasswordInputLocator = (By.XPATH, "//input[@id='password']")
    SubmitButtonLocator = (By.XPATH, "//button[@id='submit']")

    def get_ready_condition(self):
        return ec_and(
            EC.title_contains("Log In"),
            EC.presence_of_element_located(self.UsernameInputLocator),
            EC.presence_of_element_located(self.PasswordInputLocator),
            EC.presence_of_element_located(self.SubmitButtonLocator)
        )

    def login(self, username: str, password: str):
        username_field = self.driver.find_element(*self.UsernameInputLocator)
        username_field.send_keys(username)
        
        password_field = self.driver.find_element(*self.PasswordInputLocator)
        password_field.send_keys(password)

        submit_button = self.driver.find_element(*self.SubmitButtonLocator)
        submit_button.click()
```

The `ec_and` function used here is defined in [selenium.py](../selenium.py),
and can be used to compose Selenium conditions (along with `ec_or`, etc.) 
One is free to use the `PageState` objects to define useful operations 
that can happen in the state, e.g. the `login` method shown above.

## State Machine Definition

Below is an example of a state machine definition:

```python
from datafeeds.common.util.pagestate.pagestate import PageStateMachine

# Assumption:
# driver = Selenium Webdriver instance (or Gridium's thin wrapper thereof)
state_machine = PageStateMachine(driver)

state_machine.add_state(
    name="init",
    action=self.init_action,
    transitions=["login"])

state_machine.add_state(
    name="login",
    page=LoginPage(driver) # Assume definition of this page object
    action=login_action, # Assume definition of this function
    transitions=["landing_page"],
    wait_time=30)

state_machine.add_state(
    name="landing_page",
    page=LandingPage(driver), # Assume definition of this page object
    action=landing_action, # Assume definition of this function
    transitions=["done"]
)

state_machine.add_state(
    name="done"
)
```

We use `add_state` to add a new state to a state machine, with various arguments described in 
detail below.

### name
Each state has a string name. This name should be unique. This name is used in the `transitions`
attribute of a state to determine legal transitions. This is a required attribute.

### page
An optional page object associated with the state. It is possible for this to be `None`.
If so, the state is considered ready by default. That is, transitions into the state will
always succeed. Otherwise, transitions into the state can only occur when the conditions
specified by `get_ready_conditions` in the `PageObject` implementation are satisfied.
Default value: `None`.

### action
The `action` attribute of a state represents an optional function that is executed 
upon entering the state. The function should accept two arguments: (1) a `WebDriver` object, 
and (2) a `PageState` object. In the case where no `PageState` is specified, the second 
argument will be passed `None`; else the second argument will hold a reference to the 
`PageState` associated with the state.

The general post-condition for the action function is: after executing, we should be able
to transition to one of the states listed in the `transitions` attribute of the current 
state. So a login action might fill in and submit a form, at which point control is passed
back to the state machine driver to determine which state to move into next.

The default value of this attribute is `None`. 

Sample action function (all this does is call a function on the 
page object associated with the state):
```python
def login_action(self, page):
    page.login(self.username, self.password)
```

### transitions
A list of strings referring to state names, specifying which states can be legally 
transitioned into from this state. Note: some care should be taken in designing
transitions to ensure mutual exclusivity. That is, at most one transition should be
possible out of a given state at a given time. Otherwise, the behavior of the 
scraper might be somewhat unpredictable. 

If the `transitions` list is empty or None, then state machine execution terminates
upon reaching this step. The default value of `transitions` is `None`.

### wait_time
The amount of time to wait while attempting a transition out of this state, in seconds.
Default value: 15 seconds.

## Other useful tidbits

### on_enter_state()

The `on_enter_state` function can be called to invoke a user-defined callback upon
entering a state. Here is an example where a callback is specified to take a 
screenshot when entering a state.

```python
def enter_state_callback(state_name):
    self.screenshot("enter_state_{}".format(state_name))

state_machine = PageStateMachine(self._driver)

state_machine.on_enter_state(enter_state_callback)
```

### validate

The validate function ensures some basic state machine properties, such as: every transition is
to a valid, existing state. These validations don't occur at state creation time, to allow forward
references to states that haven't been created yet. This might be a very lame justification. 


## Running a State Machine

To run a state machine, you set an initial state then invoke `execute`.
```python
state_machine.set_initial_state("init")
state_machine.execute()
```

The state machine driver then does the following.
1) Invoke the `on_enter_state` callback, if specified.
2) Execute the `action` associated with the current state (at first this will be the 
state specified by `set_initial_state`).
3) Attempt to transition to the next state. We iterate through the list of possible transition
states (`transitions`), and for each one, evaluate the ready condition on the page object
associated with the state (if it exists). When we find a state with a satisfied ready condition,
or a state with no page object, we immediately transition to that state. This process is retried
until a transition occurs, or we timeout (after a duration determined by the `wait_time` attribute
on the current state). If the `transitions` is empty or `None`, we terminate the state machine.
4) Go back to step one, if we didn't terminate on step 3.
## Exceptions

Note: exceptions that occur in user specified code, e.g. an action function or other callback,
will be transparently raised from the state machine, with no adulteration.

### NoInitialStateException
Raised when one tries to `execute` a state machine without setting an initial state.

### MissingStateException Throwing 
Raised when a non-existent state is discovered by the state machine, e.g. during a transition.

### TransitionTimeoutException
Raised when a transition times out.


