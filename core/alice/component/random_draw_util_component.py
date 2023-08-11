from core.alice.component.util_component import UtilComponent
from random import randint

class RandomDrawUtilComponent(UtilComponent):
    def draw(self, two_vec_list: list, capacity: int, _wached: set):
        data = []
        if capacity == 0:
            return data
        seed = randint(0, len(two_vec_list) - 1)
        data_set = two_vec_list[seed]
        for i in range(len(data_set)):
            ele = data_set[i]
            pk = ele['pk']
            if pk in _wached:
                continue
            data.append(ele)
            if len(data) == capacity:
                break
        return data


