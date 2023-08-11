from core.alice.component.util_component import UtilComponent

class RollDrawUtilComponent(UtilComponent):
    def __init__(self, pointer_seed: int, roll: bool):
        self.last_pointer_seed = pointer_seed
        self.roll = roll


    def draw(self, two_vec_list, capacity: int, _watched: dict):
        assert hasattr(self, 'strategies_count')

        data = []
        if capacity == 0:
            return data
        next_pointer_seed = self.last_pointer_seed
        if self.roll:
            next_pointer_seed = (self.last_pointer_seed + 1) % self.strategies_count
        data_set = two_vec_list[next_pointer_seed]
        for i in range(len(data_set)):
            ele = data_set[i]
            pk = ele['pk']
            if _watched.get(pk):
                continue
            data.append(ele)
            if len(data) == capacity:
                break
        return data




    def roll(self):
        pass
