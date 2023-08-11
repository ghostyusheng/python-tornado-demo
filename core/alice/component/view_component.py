from core.alice.component.component import Component
from core.alice.strategy.strategy import Strategy


class ViewComponent(Component):

    def __init__(self, strategy:Strategy):
        self.strategy = strategy
