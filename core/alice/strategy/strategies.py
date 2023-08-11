import asyncio


class Strategies:
    data = []
    mod = None

    DEFAULT = 0
    ALTERNANT = 1
    THROUGH = 2


    def __init__(self):
        self.S = []
        self.mod = []

    def setMod(self, _mod):
        self.mod.append(_mod)

    def getMod(self):
        return self.mod

    def setData(self, _data):
        self.mod = _data

    def getData(self):
        return self._data


    def append(self, strategy):
        self.S.append(strategy)
        return self


    def insert(self, pos: int, strategy):
        self.S.insert(pos, strategy)
        return self


    @classmethod
    def multiListAlternatelyMix(cls,two_vec_list: list):
        """ list权重由高到低 """
        if not two_vec_list:
            return []
        result = []
        max_list_len = max(map(lambda x: len(x), two_vec_list))
        for i in range(max_list_len):
            for lst in two_vec_list:
                if i < len(lst):
                    result.append(lst[i])
        return result


    async def apply(self,should_remove_ids, dislike):
        Q = []
        two_vec_list = []
        remove_ids = set(should_remove_ids)
        dislike_ids = set(dislike)
        res_list = await asyncio.gather(*[s.prepare() for s in self.S])
        for res in res_list:
            not_view = []
            for item in res:
                if (str(item['pk']) in remove_ids) or (str(item['pk']) in dislike_ids):
                    continue
                not_view.append(item)
                remove_ids.add(str(item['pk']))
            two_vec_list.append(not_view)
        for i, mod in enumerate(self.getMod()):
            if mod == Strategies.ALTERNANT:
                if i == 0:
                    tmp = two_vec_list[i:i+2]
                    Q.extend(Strategies.multiListAlternatelyMix(tmp))
                else:
                    tmp = two_vec_list[i+1]
                    mix = [Q, tmp]
                    Q = Strategies.multiListAlternatelyMix(mix)
            elif mod in (Strategies.DEFAULT, Strategies.THROUGH):
                if len(two_vec_list) == 1:
                    Q = two_vec_list[i]
                else:
                    if i == 0:
                        Q += two_vec_list[i] + two_vec_list[i+1]
                    else:
                        Q += two_vec_list[i+1]
        self.data = Q


    def __str__(self):
        mod_str = ""
        if self.mod == 0:
            mod_str = 'DEFAULT'
        elif self.mod == 1:
            mod_str = 'ALTERNANT'
        elif self.mod == 2:
            mod_str = 'THROUGH'
        msg = "="*25 + "MOD: {}".format(mod_str)  + "="*25 + "\n"
        for i in self.S:
            msg += "Strategy ===> {} \n".format(i.name)
        msg += "DATA ===> {}\n".format(self.data)
        msg += "=" * 63 + "\n"
        return msg
