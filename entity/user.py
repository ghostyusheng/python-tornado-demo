from entity.base import BaseEntity


class UserEntity(BaseEntity):

    def init(self, user_id, device_id):
        if user_id:
            self.uuid = f'U:{user_id}'
        else:
            self.uuid = f'D:{device_id}'
        return self

