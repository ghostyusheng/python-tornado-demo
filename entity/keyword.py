from utils import utils
from service.nlp import NlpService
from entity.base import BaseEntity
from function.function import R


class KeywordEntity(BaseEntity):

    async def init(self, keyword):
        self.keyword = keyword
        self.toLower()
        self.toClear()
        await self.toTerms()
        return self

    def toLower(self):
        self.lower = self.keyword.lower().strip()

    def toClear(self):
        self.clear = utils.keywordFilter(self.lower)

    async def toTerms(self):
        self.terms = await NlpService.instance().analyzeKeyword(self.lower, 'smart')
