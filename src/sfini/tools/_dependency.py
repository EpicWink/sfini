# --- 80 characters -----------------------------------------------------------
# Created by: Laurie 2019/01/09

"""Specify a state-machine using state dependency."""

import logging as lg

_logger = lg.getLogger(__name__)


class _State:
    def __init__(self, state, deps=None):
        self.state = state
        self.deps = deps or []


class DependencyBuilder:  # TODO: unit-test
    _state_class = _State

    def __init__(self, state_machine):
        self.state_machine = state_machine
        self._states = []

    def add_state(self, state, dependencies=None):
        self._states.append(self._state_class(state, deps=dependencies))

    def build(self):
        pass
